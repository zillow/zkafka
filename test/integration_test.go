//go:build integration
// +build integration

package test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gitlab.zgtools.net/devex/archetypes/gomods/zfmt"
	"gitlab.zgtools.net/devex/archetypes/gomods/zstreams/v4"
	"golang.org/x/sync/errgroup"
)

// TestKafkaClientsCanReadOwnWritesAndBehaveProperlyAfterRestart will test that a kafka consumer can properly read messages
// written by the kafka producer. It will additionally, confirm that when a group is restarted that it starts off where
// it left off (addressing an off by 1 bug seen with an earlier version)
//
// The following steps are followed
// 1. Create a new consumer group that is reading from the topic
// 1. Write two messages to the topic
// 1. At this point the first message should be returned to the consumer. Assert based on offsets and message payload
// 1. Close the reader
// 1. Restart a consumer (being sure to reuse the same consumer group from before)
// 1. Read another message. Assert its the second written message (first was already read and committed)
func TestKafkaClientsCanReadOwnWritesAndBehaveProperlyAfterRestart(t *testing.T) {
	ctx := context.Background()
	topic := "integration-test-topic-2" + uuid.NewString()
	bootstrapServer := getBootstrap()

	createTopic(t, bootstrapServer, topic, 1)

	groupID := uuid.NewString()

	client := zstreams.NewClient(zstreams.Config{BootstrapServers: []string{bootstrapServer}}, zstreams.LoggerOption(stdLogger{}))
	defer func() { require.NoError(t, client.Close()) }()

	writer, err := client.Writer(ctx, zstreams.ProducerTopicConfig{
		ClientID:  fmt.Sprintf("writer-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zfmt.JSONFmt,
	})
	consumerTopicConfig := zstreams.ConsumerTopicConfig{
		ClientID:  fmt.Sprintf("reader-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zfmt.JSONFmt,
		GroupID:   groupID,
		AdditionalProps: map[string]any{
			"auto.offset.reset": "earliest",
		},
	}
	reader, err := client.Reader(ctx, consumerTopicConfig)
	require.NoError(t, err)

	readResponses := make(chan struct {
		msg *zstreams.Message
		err error
	})

	// helper method for reading from topic in loop until all messages specified in map have been read
	// will signal on channel the messages read from topic
	funcReadSpecifiedMessages := func(reader zstreams.Reader, msgsToRead map[Msg]struct{}, responses chan struct {
		msg *zstreams.Message
		err error
	},
	) {
		for {
			func() {
				rmsg, errRead := reader.Read(ctx)
				defer func() {
					if rmsg == nil {
						return
					}
					rmsg.DoneWithContext(ctx)
				}()
				if errRead != nil || rmsg == nil {
					return
				}
				gotMsg := Msg{}
				err = rmsg.Decode(&gotMsg)
				if _, ok := msgsToRead[gotMsg]; ok {
					delete(msgsToRead, gotMsg)
				}
				responses <- struct {
					msg *zstreams.Message
					err error
				}{msg: rmsg, err: err}
				if len(msgsToRead) == 0 {
					return
				}
			}()
		}
	}

	// start the reader before we write messages (otherwise, since its a new consumer group, auto.offset.reset=latest will be started at an offset later than the just written messages).
	// Loop in the reader until msg1 appears
	msg1 := Msg{Val: "1"}
	go funcReadSpecifiedMessages(reader, map[Msg]struct{}{
		msg1: {},
	}, readResponses)

	// write msg1, and msg2
	resWrite1, err := writer.Write(ctx, msg1)
	require.NoError(t, err)

	msg2 := Msg{Val: "2"}
	resWrite2, err := writer.Write(ctx, msg2)
	require.NoError(t, err)

	// reader will send on channel the messages it has read (should just be msg1)
	resp := <-readResponses
	rmsg1 := resp.msg

	require.NoError(t, resp.err)
	require.NotNil(t, rmsg1, "expected written message to be read")
	require.Equal(t, int(rmsg1.Offset), int(resWrite1.Offset), "expected read offset to match written")

	gotMsg1 := Msg{}
	err = resp.msg.Decode(&gotMsg1)

	require.NoError(t, err)
	assertEqual(t, gotMsg1, msg1)

	// close consumer so we can test a new consumer (same group) on restart
	err = reader.Close()
	require.NoError(t, err)

	// create another reader (consumer) with same consumer group from before.
	reader2, err := client.Reader(ctx, consumerTopicConfig)
	require.NoError(t, err)

	go funcReadSpecifiedMessages(reader2, map[Msg]struct{}{
		msg2: {},
	}, readResponses)
	resp2 := <-readResponses
	rmsg2 := resp2.msg

	require.NoError(t, resp2.err)
	require.NotNil(t, rmsg2, "expected written message to be read")

	// assert offset is for second message written (no replay of old message)
	require.Equal(t, int(rmsg2.Offset), int(resWrite2.Offset), "expected read offset to match written")

	gotMsg2 := Msg{}
	err = rmsg2.Decode(&gotMsg2)

	require.NoError(t, err)
	assertEqual(t, gotMsg2, msg2)
}

// Test_RebalanceDoesntCauseDuplicateMessages given a n=messageCount messages written to a topic
// when a consumer joins and starts consuming messages and later when another consumer joins
// then there are no duplicate messages processed.
//
// This is in response to a noted issue where rebalance was prone to replayed messages.
// There are multiple versions of the tests which vary the processing duration
func Test_RebalanceDoesntCauseDuplicateMessages(t *testing.T) {
	type testCase struct {
		name               string
		processingDuration time.Duration
		messageCount       int
	}
	testCases := []testCase{
		{
			name:               "processtime-10ms",
			processingDuration: time.Millisecond * 10,
			messageCount:       1000,
		},
		{
			name:               "processtime-100ms",
			processingDuration: time.Millisecond * 100,
			messageCount:       200,
		},
		{
			name:               "processtime-1000ms",
			processingDuration: time.Millisecond * 1000,
			messageCount:       50,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			ctx := context.Background()

			groupID := uuid.NewString()
			bootstrapServer := getBootstrap()

			// create a randomly named topic so we don't interfere with other tests
			topic := "topic-" + uuid.NewString()
			createTopic(t, bootstrapServer, topic, 2)

			l := stdLogger{}
			wclient := zstreams.NewClient(zstreams.Config{BootstrapServers: []string{bootstrapServer}})
			defer func() { require.NoError(t, wclient.Close()) }()
			client := zstreams.NewClient(zstreams.Config{BootstrapServers: []string{bootstrapServer}}, zstreams.LoggerOption(l))
			defer func() { require.NoError(t, client.Close()) }()

			writer, err := wclient.Writer(ctx, zstreams.ProducerTopicConfig{
				ClientID:  fmt.Sprintf("writer-%s-%s", t.Name(), uuid.NewString()),
				Topic:     topic,
				Formatter: zfmt.JSONFmt,
			})
			require.NoError(t, err)

			msg := Msg{
				Val: "sdfds",
			}

			// write N messages to topic
			msgCount := tc.messageCount
			writtenMsgs := map[int32][]int{}
			for i := 0; i < msgCount; i++ {
				msgResp, err := writer.WriteKey(ctx, uuid.NewString(), msg)
				writtenMsgs[msgResp.Partition] = append(writtenMsgs[msgResp.Partition], int(msgResp.Offset))
				require.NoError(t, err)
			}
			t.Log("Completed writing messages")

			// create work1 which has its own processor
			cTopicCfg1 := zstreams.ConsumerTopicConfig{
				ClientID:  fmt.Sprintf("reader-%s-%s", t.Name(), tc.name),
				Topic:     topic,
				Formatter: zfmt.JSONFmt,
				GroupID:   groupID,
				AdditionalProps: map[string]any{
					"auto.offset.reset": "earliest",
				},
			}
			wf := zstreams.NewWorkFactory(client, zstreams.WithLogger(l))

			maxDurationMillis := int(tc.processingDuration / time.Millisecond)
			minDurationMillis := int(tc.processingDuration / time.Millisecond)
			ctx1, cancel1 := context.WithCancel(ctx)
			defer cancel1()
			processor1 := &Processor{maxDurationMillis: maxDurationMillis, minDurationMillis: minDurationMillis, l: zstreams.NoopLogger{}}
			processor2 := &Processor{maxDurationMillis: maxDurationMillis, minDurationMillis: minDurationMillis, l: zstreams.NoopLogger{}}

			work1 := wf.Create(cTopicCfg1, processor1,
				zstreams.WithLifecycleHooks(zstreams.LifecycleHooks{PostFanout: func(ctx context.Context) {
					if len(processor1.ProcessedMessages()) > 3 && len(processor2.ProcessedMessages()) > 3 {
						cancel1()
					}
				}}))

			// create work2 which has its own processor
			ctx2, cancel2 := context.WithCancel(ctx)
			defer cancel2()
			cTopicCfg2 := cTopicCfg1
			cTopicCfg2.ClientID += "-2"
			work2 := wf.Create(cTopicCfg2, processor2,
				zstreams.WithLifecycleHooks(zstreams.LifecycleHooks{PostFanout: func(ctx context.Context) {
					totalProcessed := len(processor1.ProcessedMessages()) + len(processor2.ProcessedMessages())
					if totalProcessed == tc.messageCount {
						cancel2()
					}
				}}))

			t.Log("starting work1")
			grp := errgroup.Group{}
			grp.Go(func() error {
				if err := work1.Run(context.Background(), ctx1.Done()); err != nil {
					return err
				}
				// close reader client so work1 gracefully leaves consumergroup and work2 can finish where it left off.
				r1, err := client.Reader(context.Background(), cTopicCfg1)
				if err != nil {
					return err
				}
				return r1.Close()
			})

			// wait until processor1 has begun to process messages
			for {
				if len(processor1.ProcessedMessages()) > 5 {
					break
				}
				time.Sleep(time.Millisecond)
			}

			t.Log("starting work2")
			grp.Go(func() error {
				return work2.Run(context.Background(), ctx2.Done())
			})

			t.Log("starting exit polling")
			grp.Go(func() error {
				for {
					select {
					case <-time.After(3 * time.Minute):
						return errors.New("test could not complete before timeout")
					case <-ctx2.Done():
						if err1 := ctx1.Err(); err1 != nil {
							return nil
						}
					}
				}
			})
			err = grp.Wait()
			require.NoError(t, err, "Dont expect error returned from workers or poller")
			t.Log("work1 and work2 complete")

			// keep track of how many times each message (identified by the topic/partition/offset) is processed
			messageProcessCounter := make(map[partition]int)
			updateProcessCounter := func(msgs []*zstreams.Message) {
				for _, m := range msgs {
					key := partition{
						partition: m.Partition,
						offset:    m.Offset,
						topic:     m.Topic,
					}
					if _, ok := messageProcessCounter[key]; !ok {
						messageProcessCounter[key] = 0
					}
					messageProcessCounter[key] += 1
				}
			}
			updateProcessCounter(processor1.ProcessedMessages())
			updateProcessCounter(processor2.ProcessedMessages())

			for key, val := range messageProcessCounter {
				require.GreaterOrEqual(t, val, 1, fmt.Sprintf("Message Processed More than Once: partition %d, offset %d, topic %s", key.partition, key.offset, key.topic))
			}
			// organized processor1 and processor2 messages into a single map of partition to sorted offset.
			// Convenient for comparison with the written messages (which are already sorted)
			processedMessages := append(processor1.ProcessedMessages(), processor2.ProcessedMessages()...)
			perPartitionMessages := map[int32][]int{}
			for _, m := range processedMessages {
				perPartitionMessages[m.Partition] = append(perPartitionMessages[m.Partition], int(m.Offset))
			}
			for p := range perPartitionMessages {
				slices.Sort(perPartitionMessages[p])
			}
			require.Equal(t, writtenMsgs, perPartitionMessages, "Expected every message written to topic be processed (no misses)")
		})
	}
}

// Test_WithMultipleTopics_RebalanceDoesntCauseDuplicateMessages given a n=messageCount messages written to two topics
// when a consumer joins and starts consuming messages and later when another consumer joins
// then there are no duplicate messages processed.
func Test_WithMultipleTopics_RebalanceDoesntCauseDuplicateMessages(t *testing.T) {
	type testCase struct {
		name               string
		processingDuration time.Duration
		messageCount       int
	}
	testCases := []testCase{
		{
			name:               "processtime-10ms",
			processingDuration: time.Millisecond * 10,
			messageCount:       1000,
		},
		{
			name:               "processtime-200ms",
			processingDuration: time.Millisecond * 200,
			messageCount:       300,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			groupID := uuid.NewString()
			bootstrapServer := getBootstrap()

			// create a randomly named topic1 so we don't interfere with other tests
			topic1 := "topic1-" + uuid.NewString()
			topic2 := "topic2-" + uuid.NewString()
			createTopic(t, bootstrapServer, topic1, 2)
			createTopic(t, bootstrapServer, topic2, 2)

			l := stdLogger{}
			wclient := zstreams.NewClient(zstreams.Config{BootstrapServers: []string{bootstrapServer}})
			defer func() { require.NoError(t, wclient.Close()) }()

			writer1, err := wclient.Writer(ctx, zstreams.ProducerTopicConfig{
				ClientID:            fmt.Sprintf("writer1-%s-%s", t.Name(), tc.name),
				Topic:               topic1,
				Formatter:           zfmt.JSONFmt,
				RequestRequiredAcks: ptr("0"),
				EnableIdempotence:   ptr(false),
			})
			require.NoError(t, err)

			writer2, err := wclient.Writer(ctx, zstreams.ProducerTopicConfig{
				ClientID:            fmt.Sprintf("writer2-%s-%s", t.Name(), tc.name),
				Topic:               topic2,
				Formatter:           zfmt.JSONFmt,
				RequestRequiredAcks: ptr("0"),
				EnableIdempotence:   ptr(false),
			})
			require.NoError(t, err)

			msg := Msg{
				Val: "sdfds",
			}

			t.Log("Begin writing to Test Topic")
			// write N messages to topic1
			msgCount := tc.messageCount
			for i := 0; i < msgCount; i++ {
				_, err = writer1.WriteKey(ctx, uuid.NewString(), msg)
				require.NoError(t, err)

				_, err = writer2.WriteKey(ctx, uuid.NewString(), msg)
				require.NoError(t, err)
			}

			cTopicCfg1 := zstreams.ConsumerTopicConfig{
				ClientID:  fmt.Sprintf("dltReader-%s-%s", t.Name(), tc.name),
				Topics:    []string{topic1, topic2},
				Formatter: zfmt.JSONFmt,
				GroupID:   groupID,
				AdditionalProps: map[string]any{
					"auto.offset.reset": "earliest",
				},
			}

			client := zstreams.NewClient(zstreams.Config{BootstrapServers: []string{bootstrapServer}}, zstreams.LoggerOption(l))
			defer func() { require.NoError(t, client.Close()) }()

			wf := zstreams.NewWorkFactory(client, zstreams.WithLogger(l))

			maxDurationMillis := int(tc.processingDuration / time.Millisecond)
			minDurationMillis := int(tc.processingDuration / time.Millisecond)
			processor1 := &Processor{maxDurationMillis: maxDurationMillis, minDurationMillis: minDurationMillis, l: zstreams.NoopLogger{}}
			processor2 := &Processor{maxDurationMillis: maxDurationMillis, minDurationMillis: minDurationMillis, l: zstreams.NoopLogger{}}
			breakProcessingCondition := func() bool {
				uniqueTopics1 := map[string]struct{}{}
				for _, m := range processor1.ProcessedMessages() {
					uniqueTopics1[m.Topic] = struct{}{}
				}
				uniqueTopics2 := map[string]struct{}{}
				for _, m := range processor2.ProcessedMessages() {
					uniqueTopics2[m.Topic] = struct{}{}
				}
				return len(uniqueTopics1) > 1 && len(uniqueTopics2) > 1
			}
			ctx1, cancel1 := context.WithCancel(ctx)
			work1 := wf.Create(cTopicCfg1, processor1,
				zstreams.WithLifecycleHooks(zstreams.LifecycleHooks{PostFanout: func(ctx context.Context) {
					if breakProcessingCondition() {
						cancel1()
					}
				}}))

			cTopicCfg2 := cTopicCfg1
			cTopicCfg2.ClientID = fmt.Sprintf("dltReader2-%s-%s", t.Name(), tc.name)
			//cTopicCfg2.ClientID += "-2"
			ctx2, cancel2 := context.WithCancel(ctx)
			work2 := wf.Create(cTopicCfg2, processor2,
				zstreams.WithLifecycleHooks(zstreams.LifecycleHooks{PostFanout: func(ctx context.Context) {
					if breakProcessingCondition() {
						cancel2()
					}
				}}))

			// break out of processing loop when a message has been processed from each subscribed topic

			t.Log("starting work1")
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				work1.Run(ctx1, nil)
			}()

			// wait until processor1 has begun to process messages
			for {
				if len(processor1.ProcessedMessages()) > 5 {
					break
				}
				time.Sleep(time.Millisecond)
			}

			t.Log("starting work2")
			wg.Add(1)
			go func() {
				defer wg.Done()
				work2.Run(ctx2, nil)
			}()
			wg.Wait()

			messageProcessCounter := make(map[partition]int)
			updateProcessCounter := func(msgs []*zstreams.Message) {
				for _, m := range msgs {
					key := partition{
						partition: m.Partition,
						offset:    m.Offset,
						topic:     m.Topic,
					}
					if _, ok := messageProcessCounter[key]; !ok {
						messageProcessCounter[key] = 0
					}
					messageProcessCounter[key] += 1
				}
			}
			updateProcessCounter(processor1.ProcessedMessages())
			updateProcessCounter(processor2.ProcessedMessages())

			for key, val := range messageProcessCounter {
				require.GreaterOrEqual(t, val, 1, fmt.Sprintf("Message Processed More than Once: partition %d, offset %d, topic %s", key.partition, key.offset, key.topic))
			}
		})
	}
}

// Test_WithConcurrentProcessing_RebalanceDoesntCauseDuplicateMessages given n=messageCount messages written to a topic
// when a consumer joins and starts consuming messages (concurrently) and later when another consumer joins (concurrently)
// then there are no duplicate messages processed.
//
// The consumer's processing times are set to a range as opposed to a specific duration. This allows lookahead processing (where messages
// of higher offsets are processed and completed, potentially, before lower offsets
func Test_WithConcurrentProcessing_RebalanceDoesntCauseDuplicateMessages(t *testing.T) {
	type testCase struct {
		name                        string
		processingDurationMinMillis int
		processingDurationMaxMillis int
		messageCount                int
	}
	testCases := []testCase{
		{
			name:                        "processtime-200ms",
			processingDurationMinMillis: 0,
			processingDurationMaxMillis: 400,
			messageCount:                300,
		},
		{
			name:                        "processtime-1000ms",
			processingDurationMinMillis: 800,
			processingDurationMaxMillis: 1200,
			messageCount:                100,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			groupID := uuid.NewString()
			bootstrapServer := getBootstrap()

			// create a randomly named topic so we don't interfere with other tests
			topic := "topic-" + uuid.NewString()
			t.Logf("Creating topic %s", topic)
			createTopic(t, bootstrapServer, topic, 2)
			t.Log("Completed creating topic")

			l := zstreams.NoopLogger{}
			wclient := zstreams.NewClient(zstreams.Config{BootstrapServers: []string{bootstrapServer}})
			defer func() { require.NoError(t, wclient.Close()) }()

			writer, err := wclient.Writer(ctx, zstreams.ProducerTopicConfig{
				ClientID:            fmt.Sprintf("writer-%s-%s", t.Name(), tc.name),
				Topic:               topic,
				Formatter:           zfmt.JSONFmt,
				RequestRequiredAcks: ptr("0"),
				EnableIdempotence:   ptr(false),
			})
			require.NoError(t, err)

			msg := Msg{
				Val: "sdfds",
			}

			// write N messages to topic
			t.Logf("Writing n = %d", tc.messageCount)
			msgCount := tc.messageCount
			for i := 0; i < msgCount; i++ {
				_, err = writer.WriteKey(ctx, uuid.NewString(), msg)
				require.NoError(t, err)
			}
			t.Logf("Completed writing n message")

			cTopicCfg1 := zstreams.ConsumerTopicConfig{
				ClientID:  fmt.Sprintf("dltReader-%s-%s", t.Name(), tc.name),
				Topic:     topic,
				Formatter: zfmt.JSONFmt,
				GroupID:   groupID,
				AdditionalProps: map[string]any{
					"auto.offset.reset": "earliest",
				},
			}

			client := zstreams.NewClient(zstreams.Config{BootstrapServers: []string{bootstrapServer}}, zstreams.LoggerOption(l))
			defer func() { require.NoError(t, client.Close()) }()

			wf := zstreams.NewWorkFactory(client, zstreams.WithLogger(l))

			processor1 := &Processor{minDurationMillis: tc.processingDurationMinMillis, maxDurationMillis: tc.processingDurationMaxMillis, l: l}
			processor2 := &Processor{minDurationMillis: tc.processingDurationMinMillis, maxDurationMillis: tc.processingDurationMaxMillis, l: l}
			breakProcessingCondition := func() bool {
				return len(processor1.ProcessedMessages()) > 3 && len(processor2.ProcessedMessages()) > 3
			}
			ctx1, cancel1 := context.WithCancel(ctx)
			work1 := wf.Create(cTopicCfg1, processor1, zstreams.Speedup(5),
				zstreams.WithLifecycleHooks(zstreams.LifecycleHooks{PostFanout: func(ctx context.Context) {
					if breakProcessingCondition() {
						cancel1()
					}
				}}))

			cTopicCfg2 := cTopicCfg1
			cTopicCfg2.ClientID += "-2"
			ctx2, cancel2 := context.WithCancel(ctx)
			work2 := wf.Create(cTopicCfg2, processor2, zstreams.Speedup(5),
				zstreams.WithLifecycleHooks(zstreams.LifecycleHooks{PostFanout: func(ctx context.Context) {
					if breakProcessingCondition() {
						cancel2()
					}
				}}))

			t.Log("starting work1")
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				work1.Run(ctx1, nil)
			}()

			// wait until processor1 has begun to process messages
			for {
				if len(processor1.ProcessedMessages()) > 5 {
					break
				}
				time.Sleep(time.Millisecond)
			}

			t.Log("starting work2")
			wg.Add(1)
			go func() {
				defer wg.Done()
				work2.Run(ctx2, nil)
			}()
			wg.Wait()
			// keep track of how many messages
			messageProcessCounter := make(map[partition]int)
			updateProcessCounter := func(msgs []*zstreams.Message) {
				for _, m := range msgs {
					key := partition{
						partition: m.Partition,
						offset:    m.Offset,
						topic:     m.Topic,
					}
					if _, ok := messageProcessCounter[key]; !ok {
						messageProcessCounter[key] = 0
					}
					messageProcessCounter[key] += 1
				}
			}
			updateProcessCounter(processor1.ProcessedMessages())
			updateProcessCounter(processor2.ProcessedMessages())

			for key, val := range messageProcessCounter {
				if val > 1 {
					t.Errorf("Message Processed More than Once: partition %d, offset %d, topic %s", key.partition, key.offset, key.topic)
				}
			}
		})
	}
}

// Test_AssignmentsReflectsConsumerAssignments given a single consumer in a group all the partitions (2) are assigned to them
// when another consumer joins the group
// then a rebalance occurs and the partitions are split between the two and this is reflected in the Assignments call
//
// The rebalances are handled during the Poll call under the hood (which is only called while a KReader is in the attempt of Reading.
// So as we simulate two members of a group we'll need to keep calling from both consumers so the rebalance eventually occurs
func Test_AssignmentsReflectsConsumerAssignments(t *testing.T) {
	ctx := context.Background()

	groupID := uuid.NewString()
	bootstrapServer := getBootstrap()

	// create a randomly named topic so we don't interfere with other tests
	topic := "topic-" + uuid.NewString()
	partitionCount := 2
	createTopic(t, bootstrapServer, topic, partitionCount)

	l := zstreams.NoopLogger{}
	wclient := zstreams.NewClient(zstreams.Config{BootstrapServers: []string{bootstrapServer}})
	defer func() { require.NoError(t, wclient.Close()) }()

	client := zstreams.NewClient(zstreams.Config{BootstrapServers: []string{bootstrapServer}}, zstreams.LoggerOption(l))
	defer func() { require.NoError(t, client.Close()) }()

	writer, err := wclient.Writer(ctx, zstreams.ProducerTopicConfig{
		ClientID:  fmt.Sprintf("writer-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zfmt.JSONFmt,
	})

	require.NoError(t, err)

	msg := Msg{
		Val: "sdfds",
	}

	t.Log("Begin writing messages")
	// write N messages to topic
	msgCount := 40
	for i := 0; i < msgCount; i++ {
		_, err = writer.WriteKey(ctx, uuid.NewString(), msg)
		require.NoError(t, err)
	}

	t.Log("Completed writing messages")

	// create consumer 1 which has its own processor
	cTopicCfg1 := zstreams.ConsumerTopicConfig{
		ClientID:  fmt.Sprintf("reader-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zfmt.JSONFmt,
		GroupID:   groupID,
		// use increase readtimeout so less likely for reader1 to finish processing before r2 joins.
		ReadTimeoutMillis: ptr(5000),
		AdditionalProps: map[string]any{
			"auto.offset.reset": "earliest",
		},
	}

	reader1, err := client.Reader(ctx, cTopicCfg1)
	require.NoError(t, err)
	r1, ok := reader1.(*zstreams.KReader)
	require.True(t, ok, "expected reader to be KReader")

	// create consumer 2 which has its own processor
	cTopicCfg2 := cTopicCfg1
	cTopicCfg2.ClientID += "-2"
	reader2, err := client.Reader(ctx, cTopicCfg2)
	require.NoError(t, err)
	r2, ok := reader2.(*zstreams.KReader)
	require.True(t, ok, "expected reader to be KReader")

	// a helper method for reading a message and committing if its available (similar to what the work loop would do)
	readAndCommit := func(t *testing.T, r *zstreams.KReader) *zstreams.Message {
		t.Helper()
		msg, err := r.Read(context.Background())
		require.NoError(t, err)
		if msg != nil {
			msg.Done()
		}
		return msg
	}

	// wait until a message is consumed (meaning the consumer has joined the group
	for {
		msg1 := readAndCommit(t, r1)
		if msg1 != nil {
			break
		}
	}

	assignments, err := r1.Assignments(ctx)
	require.NoError(t, err)
	require.Equal(t, partitionCount, len(assignments), "expected all partitions to be assigned to consumer 1")

	t.Log("Begin attempted consumption by consumer 2. Should result in rebalance")
	// now start trying to consume from both consumer 1 and 2. Eventually both will join group (Break when second consumer has joined group, signaled by processing a message)
	for {
		msg2 := readAndCommit(t, r2)
		if msg2 != nil {
			_ = readAndCommit(t, r1)
			break
		}
		msg3 := readAndCommit(t, r1)
		require.NotNil(t, msg3, "test can deadlock if reader1 finishes processing all messages before reader2 joins group. We'll fail if reader1 exhausts all its messages")
	}

	assignments1, err := r1.Assignments(ctx)
	require.NoError(t, err)

	assignments2, err := r2.Assignments(ctx)
	require.NoError(t, err)

	// expect assignments to be split amongst the two consumers
	require.Len(t, assignments1, 1, "unexpected number of assignments")
	require.Len(t, assignments2, 1, "unexpected number of assignments")
}

// Test_UnfinishableWorkDoesntBlockWorkIndefinitely given two consumers 1 with unfinished work
// consuming from a topic that has multiple partitions (so both can simultaneously consume)
// when the second consumer joins and causes a rebalance
// then the first isn't infinitely blocked in its rebalance
func Test_UnfinishableWorkDoesntBlockWorkIndefinitely(t *testing.T) {
	ctx := context.Background()

	groupID := uuid.NewString()
	bootstrapServer := getBootstrap()

	// create a randomly named topic so we don't interfere with other tests
	topic := "topic-" + uuid.NewString()
	partitionCount := 4
	createTopic(t, bootstrapServer, topic, partitionCount)

	l := stdLogger{}
	wclient := zstreams.NewClient(zstreams.Config{BootstrapServers: []string{bootstrapServer}})
	defer func() { require.NoError(t, wclient.Close()) }()
	client := zstreams.NewClient(zstreams.Config{BootstrapServers: []string{bootstrapServer}}, zstreams.LoggerOption(l))
	defer func() { require.NoError(t, client.Close()) }()

	writer, err := wclient.Writer(ctx, zstreams.ProducerTopicConfig{
		ClientID:            fmt.Sprintf("writer-%s-%s", t.Name(), uuid.NewString()),
		Topic:               topic,
		Formatter:           zfmt.JSONFmt,
		RequestRequiredAcks: ptr("0"),
		EnableIdempotence:   ptr(false),
	})
	require.NoError(t, err)

	msg := Msg{
		Val: "sdfds",
	}

	// write N messages to topic
	msgCount := 40
	for i := 0; i < msgCount; i++ {
		_, err = writer.WriteKey(ctx, uuid.NewString(), msg)
		require.NoError(t, err)
	}

	// create consumer 1 which has its own processor
	cTopicCfg1 := zstreams.ConsumerTopicConfig{
		ClientID:          fmt.Sprintf("reader-%s-%s", t.Name(), uuid.NewString()),
		Topic:             topic,
		Formatter:         zfmt.JSONFmt,
		GroupID:           groupID,
		ReadTimeoutMillis: ptr(5000),
		AdditionalProps: map[string]any{
			"auto.offset.reset": "earliest",
		},
	}

	reader1, err := client.Reader(ctx, cTopicCfg1)
	require.NoError(t, err)

	r1, ok := reader1.(*zstreams.KReader)
	require.True(t, ok, "expected reader to be KReader")

	// create consumer 2 which has its own processor
	cTopicCfg2 := cTopicCfg1
	cTopicCfg2.ClientID += "-2"
	reader2, err := client.Reader(ctx, cTopicCfg2)
	require.NoError(t, err)

	r2, ok := reader2.(*zstreams.KReader)
	require.True(t, ok, "expected reader to be KReader")
	defer client.Close()

	// wait until a message is consumed (meaning the consumer has joined the group) but dont commit it
	// this will be marked as unfinished work. This will cause issues on rebalance but shouldn't block it forever
	for {
		msg1, err := r1.Read(ctx)
		require.NoError(t, err)
		if msg1 != nil {
			break
		}
	}
	t.Log("Successfully read 1 message with reader 1")

	// now start trying to consume from both consumer 1 and 2.
	// Eventually both will join group. If rebalance isn't properly implemented with timeout, reader1 could block forever on account of
	// unfinished work
	for {
		msg2, err := r2.Read(ctx)
		require.NoError(t, err)
		if msg2 != nil {
			break
		}
		t.Log("Haven't read message with reader 2 yet")
		_, err = r1.Read(ctx)
		require.NoError(t, err)
	}
}

// TestKafkaClientsCanWriteToTheirDeadLetterTopic given a message in a source topic
// when processing that message errors and a deadletter is configured
// then the errored message will be written to the dlt
func Test_KafkaClientsCanWriteToTheirDeadLetterTopic(t *testing.T) {
	bootstrapServer := getBootstrap()
	topic := "topic1" + uuid.NewString()
	dlt := "deadlettertopic1" + uuid.NewString()

	createTopic(t, bootstrapServer, topic, 2)
	createTopic(t, bootstrapServer, dlt, 2)

	groupID := uuid.NewString()

	client := zstreams.NewClient(zstreams.Config{BootstrapServers: []string{bootstrapServer}}, zstreams.LoggerOption(stdLogger{}))
	defer func() { require.NoError(t, client.Close()) }()

	ctx := context.Background()

	writer, err := client.Writer(ctx, zstreams.ProducerTopicConfig{
		ClientID:  fmt.Sprintf("writer-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zfmt.JSONFmt,
	})

	consumerTopicConfig := zstreams.ConsumerTopicConfig{
		ClientID:  fmt.Sprintf("worker-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zfmt.JSONFmt,
		GroupID:   groupID,
		AdditionalProps: map[string]any{
			"auto.offset.reset": "earliest",
		},
		DeadLetterTopicConfig: &zstreams.ProducerTopicConfig{
			ClientID:  fmt.Sprintf("dltWriter-%s-%s", t.Name(), uuid.NewString()),
			Topic:     dlt,
			Formatter: zfmt.JSONFmt,
		},
	}

	msg := Msg{
		Val: "sdfds",
	}

	key := "original-key"
	_, err = writer.WriteKey(ctx, key, msg)
	require.NoError(t, err)

	wf := zstreams.NewWorkFactory(client)

	processor := &Processor{
		reterr: errors.New("processing error"),
	}
	work := wf.Create(consumerTopicConfig, processor)

	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		work.Run(ctx2, nil)
	}()
	for {
		if len(processor.ProcessedMessages()) == 1 {
			cancel()
			break
		}
		time.Sleep(time.Millisecond)
	}

	dltReader, err := client.Reader(ctx, zstreams.ConsumerTopicConfig{
		ClientID:          fmt.Sprintf("reader-for-test-%s-%s", t.Name(), uuid.NewString()),
		GroupID:           uuid.NewString(),
		Topic:             dlt,
		Formatter:         zfmt.JSONFmt,
		ReadTimeoutMillis: ptr(15000),
		AdditionalProps: map[string]any{
			"auto.offset.reset": "earliest",
		},
	})

	require.NoError(t, err)

	t.Log("reading from dlt")
	// read message from dlt to confirm it was forwarded
	dltMsgWrapper, err := dltReader.Read(ctx)
	require.NoError(t, err)
	require.NotNil(t, dltMsgWrapper)

	got := Msg{}
	err = dltMsgWrapper.Decode(&got)
	require.NoError(t, err)

	t.Log("assert message from dlt")
	assertEqual(t, got, msg)
	assertEqual(t, dltMsgWrapper.Key, key)
}

func Test_WorkDelay_GuaranteesProcessingDelayedAtLeastSpecifiedDelayDurationFromWhenMessageWritten(t *testing.T) {
	ctx := context.Background()

	groupID := uuid.NewString()
	bootstrapServer := getBootstrap()

	// create a randomly named topic so we don't interfere with other tests
	topic := "topic-" + uuid.NewString()
	createTopic(t, bootstrapServer, topic, 2)

	l := stdLogger{}
	wclient := zstreams.NewClient(zstreams.Config{BootstrapServers: []string{bootstrapServer}})
	defer func() { require.NoError(t, wclient.Close()) }()
	client := zstreams.NewClient(zstreams.Config{BootstrapServers: []string{bootstrapServer}}, zstreams.LoggerOption(l))
	defer func() { require.NoError(t, client.Close()) }()

	processDelayMillis := 2000
	// create work which has its own processor
	cTopicCfg1 := zstreams.ConsumerTopicConfig{
		ClientID:           fmt.Sprintf("reader-%s-%s", t.Name(), uuid.NewString()),
		Topic:              topic,
		Formatter:          zfmt.JSONFmt,
		GroupID:            groupID,
		ProcessDelayMillis: &processDelayMillis,
		AdditionalProps: map[string]any{
			"auto.offset.reset": "earliest",
		},
	}
	wf := zstreams.NewWorkFactory(client, zstreams.WithLogger(l))

	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()

	type result struct {
		message           *zstreams.Message
		processingInstant time.Time
	}
	var results []result
	processor1 := &fakeProcessor{
		process: func(ctx context.Context, message *zstreams.Message) error {
			results = append(results, result{
				message:           message,
				processingInstant: time.Now(),
			})
			return nil
		},
	}

	work := wf.Create(cTopicCfg1, processor1)

	t.Log("starting work")
	grp := errgroup.Group{}
	grp.Go(func() error {
		return work.Run(context.Background(), ctx1.Done())
	})

	writer, err := wclient.Writer(ctx, zstreams.ProducerTopicConfig{
		ClientID:  fmt.Sprintf("writer-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zfmt.JSONFmt,
	})
	require.NoError(t, err)

	msg := Msg{
		Val: "sdfds",
	}

	t.Log("Started writing messages")
	// write N messages to topic
	msgCount := 1000
	for i := 0; i < msgCount; i++ {
		_, err = writer.Write(ctx, msg)
		require.NoError(t, err)
	}
	t.Log("Completed writing messages")

	t.Log("starting exit polling")
	pollWait(func() bool {
		return len(results) >= msgCount
	},
		pollOpts{
			exit: cancel1,
			timeoutExit: func() {
				require.Failf(t, "Polling condition not met prior to test timeout", "Processing count %s", len(results))
			},
		},
	)
	err = grp.Wait()
	require.NoError(t, err)

	t.Log("work complete")
	for _, r := range results {
		require.NotEmpty(t, r.processingInstant)
		require.NotEmpty(t, r.message.TimeStamp)
		require.GreaterOrEqual(t, r.processingInstant, r.message.TimeStamp.Add(time.Duration(processDelayMillis)*time.Millisecond), "Expect processing time to be equal to the write timestamp plus the work delay duration")
	}
}

// Test_WorkDelay_DoesntHaveDurationStackEffect confirms that a work doesn't unnecessarily add delay (only pausing a partition when a message was written within the last `duration` moments).
// A bad implementation of work delay would call that pause for each message, and in a built up topic, this would lead to increased perceived latency.
//
// This test creates N messages on the topic and then starts processing
// 1. It asserts that the time since the message was written is at least that of the delay.
// This is a weak assertion sicne the messages are written before the work consumer group is started. Other tests do a better job confirming this behavior
// 2. It also asserts that the time between the first and last message is very short.
// This is expected in a backlog situation, since the worker will delay once, and with monotonically increasing timestamps won't have to dely again
func Test_WorkDelay_DoesntHaveDurationStackEffect(t *testing.T) {
	ctx := context.Background()

	groupID := uuid.NewString()
	bootstrapServer := getBootstrap()

	// create a randomly named topic so we don't interfere with other tests
	topic := "topic-" + uuid.NewString()
	createTopic(t, bootstrapServer, topic, 2)

	l := stdLogger{}
	wclient := zstreams.NewClient(zstreams.Config{BootstrapServers: []string{bootstrapServer}})
	defer func() { require.NoError(t, wclient.Close()) }()
	client := zstreams.NewClient(zstreams.Config{BootstrapServers: []string{bootstrapServer}}, zstreams.LoggerOption(l))
	defer func() { require.NoError(t, client.Close()) }()

	writer, err := wclient.Writer(ctx, zstreams.ProducerTopicConfig{
		ClientID:  fmt.Sprintf("writer-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zfmt.JSONFmt,
	})
	require.NoError(t, err)

	msg := Msg{
		Val: "sdfds",
	}

	t.Log("Started writing messages")
	// write N messages to topic
	msgCount := 500
	for i := 0; i < msgCount; i++ {
		_, err = writer.Write(ctx, msg)
		require.NoError(t, err)
	}
	t.Log("Completed writing messages")

	processDelayMillis := 2000
	// create work which has its own processor
	cTopicCfg1 := zstreams.ConsumerTopicConfig{
		ClientID:           fmt.Sprintf("reader-%s-%s", t.Name(), uuid.NewString()),
		Topic:              topic,
		Formatter:          zfmt.JSONFmt,
		GroupID:            groupID,
		ProcessDelayMillis: &processDelayMillis,
		AdditionalProps: map[string]any{
			"auto.offset.reset": "earliest",
		},
	}
	wf := zstreams.NewWorkFactory(client, zstreams.WithLogger(l))

	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()

	type result struct {
		message           *zstreams.Message
		processingInstant time.Time
	}
	var results []result
	processor1 := &fakeProcessor{
		process: func(ctx context.Context, message *zstreams.Message) error {
			results = append(results, result{
				message:           message,
				processingInstant: time.Now(),
			})
			return nil
		},
	}

	work := wf.Create(cTopicCfg1, processor1)

	t.Log("starting work")
	grp := errgroup.Group{}
	grp.Go(func() error {
		return work.Run(context.Background(), ctx1.Done())
	})

	t.Log("starting exit polling")
	pollWait(func() bool {
		return len(results) >= msgCount
	},
		pollOpts{
			exit: cancel1,
			timeoutExit: func() {
				require.Failf(t, "Polling condition not met prior to test timeout", "Processing count %s", len(results))
			},
		},
	)
	err = grp.Wait()
	require.NoError(t, err)

	t.Log("work complete")
	for _, r := range results {
		require.NotEmpty(t, r.processingInstant)
		require.NotEmpty(t, r.message.TimeStamp)
		require.GreaterOrEqual(t, r.processingInstant, r.message.TimeStamp.Add(time.Duration(processDelayMillis)*time.Millisecond), "Expect processing time to be equal to the write timestamp plus the work delay duration")
	}
	first := results[0]
	last := results[len(results)-1]
	require.WithinDuration(t, last.processingInstant, first.processingInstant, time.Duration(processDelayMillis/2)*time.Millisecond, "Time since first and last processed message should be very short, since processing just updates an in memory slice. This should take on the order of microseconds, but to account for scheduling drift the assertion is half the delay")
}

func createTopic(t *testing.T, bootstrapServer, topic string, partitions int) {
	t.Helper()
	aclient, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrapServer})
	require.NoError(t, err)
	_, err = aclient.CreateTopics(context.Background(), []kafka.TopicSpecification{
		{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		},
	})
	require.NoError(t, err)
}

// getBootstrap returns the kafka broker to be used for integration tests. It allows the overwrite of default via
// envvar
func getBootstrap() string {
	bootstrapServer, ok := os.LookupEnv("KAFKA_BOOTSTRAP_SERVER")
	if !ok {
		bootstrapServer = "localhost:9093" // local development
	}
	return bootstrapServer
}

type Msg struct {
	Val string
}

type Processor struct {
	m                 sync.Mutex
	processedMessages []*zstreams.Message
	reterr            error
	minDurationMillis int
	maxDurationMillis int
	l                 zstreams.Logger
}

func (p *Processor) Process(ctx context.Context, msg *zstreams.Message) error {
	p.m.Lock()
	p.processedMessages = append(p.processedMessages, msg)
	p.m.Unlock()
	if p.l != nil {
		p.l.Infow(ctx, "Process", "partition", msg.Partition, "offset", msg.Offset)
	}
	durationRange := p.maxDurationMillis - p.minDurationMillis
	delayMillis := p.minDurationMillis
	if durationRange != 0 {
		delayMillis += rand.Intn(durationRange)
	}
	delay := time.Duration(delayMillis) * time.Millisecond
	<-time.After(delay)
	return p.reterr
}

func (p *Processor) ProcessedMessages() []*zstreams.Message {
	p.m.Lock()
	defer p.m.Unlock()

	var msgs []*zstreams.Message
	for _, m := range p.processedMessages {
		msgs = append(msgs, m)
	}
	return msgs
}

func assertEqual(t *testing.T, got, want any, opts ...cmp.Option) {
	t.Helper()
	if diff := cmp.Diff(got, want, opts...); diff != "" {
		diff = fmt.Sprintf("\ngot: -\nwant: +\n%s", diff)
		t.Fatal(diff)
	}
}

type partition struct {
	partition int32
	offset    int64
	topic     string
}
