package zstreams

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/golang/mock/gomock"
	mock_confluent "gitlab.zgtools.net/devex/archetypes/gomods/zstreams/v4/mocks/confluent"
)

func TestWork_processTimeoutMillis(t *testing.T) {
	type fields struct {
		topicConfig ConsumerTopicConfig
	}
	tests := []struct {
		name   string
		fields fields
		want   time.Duration
	}{
		{
			// this case shouldn't happen as zstreams should set a default when this field is nil. But for completeness we'll include it
			name: "topic config has specified processTimeoutDuration",
			fields: fields{
				topicConfig: ConsumerTopicConfig{
					ProcessTimeoutMillis: ptr(1000),
				},
			},
			want: time.Second,
		},
		{
			// this case shouldn't happen as zstreams should set a default when this field is nil. But for completeness we'll include it
			name: "topic config has missing processTimeoutDuration",
			fields: fields{
				topicConfig: ConsumerTopicConfig{},
			},
			want: time.Second * 60,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer recoverThenFail(t)
			w := &Work{
				topicConfig: tt.fields.topicConfig,
			}
			if got := w.processTimeoutDuration(); got != tt.want {
				t.Errorf("processTimeoutDuration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWork_WithOptions(t *testing.T) {
	defer recoverThenFail(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tp := noop.TracerProvider{}
	propagator := propagation.TraceContext{}

	wf := NewWorkFactory(mockClientProvider{}, WithTracerProvider(tp), WithTextMapPropagator(propagator))

	work := wf.Create(ConsumerTopicConfig{}, &timeDelayProcessor{})

	require.Equal(t, tp.Tracer(""), work.tracer)
}

// TestWork_ShouldCommitMessagesProperly asserts the behavior of committing kafka messages.
// Messages should be committed as they complete as long as there aren't lower offset messages still in progress.
// This tests specifies processing delay times such that low offsets finish after high offsets and asserts that the storeOffsets method
// isn't called until these lower offsets complete
// In this test case we read the following {partition, offsets}:
// {1,1}, {2,1}, {1,2}, {1,3}
// and we finish processing them in the following order at time t
// {2,1} t=0, {1,3} t=0, {1,2} t=90, {1,1} t=100 (it should be noted {2,1} and {1,3} have the same specified finish time)
// and could be swapped by chance. Also, the times are using a system clock, so are subject to some wiggle. Hopefully the delays
// are large enough that we don't run into weird behaviors and the assertions can remain unchanged.
// {2,1} comes and is the only message inwork for that partition. A commit is executed
// {1,3} comes in, but {1,2} and {1,1} are still inwork. No commit done as lower offsets are inwork
// {1,2} comes in. Same story as above
// {1,1} comes in, and partition 1 can be committed. We'll only do 1 commit for the largest one {1,3}
// We'll assert that we only see two commits {2,1} and {1,3}
func TestWork_ShouldCommitMessagesProperly(t *testing.T) {
	defer recoverThenFail(t)

	type testInput struct {
		// the message to be processed
		//
		msg kafka.Message
		// a simulated process delay for the associated message
		processDelay time.Duration
	}

	topicName := "topic"
	now := time.Now()
	inputs := []testInput{
		{
			msg: kafka.Message{
				TopicPartition: kafka.TopicPartition{Partition: 1, Offset: 1, Topic: &topicName},
				Timestamp:      now,
			},
			processDelay: time.Millisecond * 100,
		},
		{
			msg: kafka.Message{
				TopicPartition: kafka.TopicPartition{Partition: 2, Offset: 1, Topic: &topicName},
				Timestamp:      now,
			},
			processDelay: time.Millisecond * 0,
		},
		{
			msg: kafka.Message{
				TopicPartition: kafka.TopicPartition{Partition: 1, Offset: 2, Topic: &topicName},
				Timestamp:      now,
			},
			processDelay: time.Millisecond * 50,
		},
		{
			msg: kafka.Message{
				TopicPartition: kafka.TopicPartition{Partition: 1, Offset: 3, Topic: &topicName},
				Timestamp:      now,
			},
			processDelay: time.Millisecond * 0,
		},
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	consumer := mock_confluent.NewMockKafkaConsumer(ctrl)
	consumer.EXPECT().SubscribeTopics(gomock.Any(), gomock.Any()).Times(1)
	var consumerCalls []*gomock.Call

	msgToDelay := make(map[key]time.Duration)
	for i := range inputs {
		input := inputs[i]
		consumerCalls = append(consumerCalls, consumer.EXPECT().ReadMessage(gomock.Any()).Return(&input.msg, nil))
		msgToDelay[key{partition: input.msg.TopicPartition.Partition, offset: int64(input.msg.TopicPartition.Offset)}] = input.processDelay
	}
	consumerCalls = append(consumerCalls, consumer.EXPECT().ReadMessage(gomock.Any()).Return(nil, nil).AnyTimes())
	gomock.InOrder(
		consumerCalls...,
	)

	m := sync.Mutex{}
	storedOffsets := map[int][]kafka.TopicPartition{}
	consumer.EXPECT().StoreOffsets(gomock.Any()).DoAndReturn(func(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
		m.Lock()
		defer m.Unlock()
		partition := int(offsets[0].Partition)
		if _, ok := storedOffsets[partition]; !ok {
			storedOffsets[partition] = nil
		}
		storedOffsets[partition] = append(storedOffsets[partition], offsets[0])
		return offsets, nil
	},
	).MaxTimes(len(inputs))

	processor := timeDelayProcessor{
		msgToDelay: msgToDelay,
	}

	l := NoopLogger{}

	r := KReader{
		tCommitMgr: newTopicCommitMgr(),
		consumer:   consumer,
		topicConfig: ConsumerTopicConfig{
			ReadTimeoutMillis: ptr(1),
		},
		logger: l,
	}

	wf := WorkFactory{
		logger: l,
		tp:     noop.TracerProvider{},
		p:      propagation.TraceContext{},
	}

	countMtx := sync.Mutex{}
	processCount := 0
	var msgTimeStamp time.Time
	// we need to be in a concurrent environment so that we can simulate work happening simultaneously and finishing in orders
	// different from how the messages are read from the topic.
	// Additionally, we'll specify a callback function which will update as messages are processed
	work := wf.Create(ConsumerTopicConfig{}, &processor, Speedup(5), WithOnDone(func(ctx context.Context, m *Message, _ error) {
		countMtx.Lock()
		processCount += 1
		msgTimeStamp = m.TimeStamp
		countMtx.Unlock()
	}))
	work.reader = &r

	// act
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		work.Run(ctx, nil)
	}()

	go func() {
		for {
			if processCount == len(inputs) {
				cancel()
			}
		}
	}()

	// However, if there's a bug in our test (or code) this might never occur.
	// We'll use this select case to specify a timeout for our tests
	select {
	case <-ctx.Done():
		break
	}

	require.Equal(t, now, msgTimeStamp, "expected timestamp in kafka.Message to be mapped zstreams.Message")

	// These are the largest offsets that are processed. They should show up last (because larger offsets shouldn't be stored
	// before smaller offsets) in stored offsets which is ordered
	// by the call order to store offsets
	expectedPartition1Offset := kafka.TopicPartition{Topic: &topicName, Partition: 1, Offset: 4}
	expectedPartition2Offset := kafka.TopicPartition{Topic: &topicName, Partition: 2, Offset: 2}
	//
	assertContains(t, expectedPartition1Offset, storedOffsets[1])
	assertContains(t, expectedPartition2Offset, storedOffsets[2])
	// last storedOffset should be equal to our expectation
	assertEqual(t, expectedPartition1Offset, storedOffsets[1][len(storedOffsets[1])-1])
	assertEqual(t, expectedPartition2Offset, storedOffsets[2][len(storedOffsets[2])-1])

	partitions := []int32{1, 2}
	for _, partition := range partitions {
		c := r.tCommitMgr.get(topicName)
		require.Contains(t, c.partitionToInWork, partition, "expect inwork message map to contain holder for visited partition")
		require.Empty(t, c.partitionToInWork[partition].data, "all messages should be purged from heap")
		require.Contains(t, c.partitionToCompleted, partition, "expect partitionToCompleted message map to contain holder for visited partition")
		require.Empty(t, c.partitionToCompleted[partition].data, "all messages should be purged from heap")
	}
}

// TestWork_CommitManagerBeEmptyAfterAllProcessingCompletes asserts that the heaps tracked by the commitManager
// is empty after the conclusion of processing. This should be true as the commitManager is only responsible for
// tracking commits that have yet to be committed (typically because they finish out of order)
func TestWork_CommitManagerIsEmptyAfterAllProcessingCompletes(t *testing.T) {
	defer recoverThenFail(t)
	type testInput struct {
		// the message to be processed
		msg kafka.Message
		// a simulated process delay for the associated message
		processDelay time.Duration
	}

	// arrange many kafka messages with random amounts of processing delay
	var inputs []testInput
	messageCount := 10000
	partitionCount := 3
	topicName := "topic-name"
	for i := 0; i < messageCount; i++ {
		randPartition := i % partitionCount
		offset := i
		randDelayMillis := rand.Intn(10)
		inputs = append(inputs, testInput{
			msg: kafka.Message{
				TopicPartition: kafka.TopicPartition{Partition: int32(randPartition), Offset: kafka.Offset(offset), Topic: &topicName},
				Timestamp:      time.Now(),
			},
			processDelay: time.Duration(randDelayMillis) * time.Millisecond,
		})
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	consumer := mock_confluent.NewMockKafkaConsumer(ctrl)
	consumer.EXPECT().SubscribeTopics(gomock.Any(), gomock.Any()).Times(1)
	var consumerCalls []*gomock.Call

	msgToDelay := make(map[key]time.Duration)
	for i := range inputs {
		input := inputs[i]
		consumerCalls = append(consumerCalls, consumer.EXPECT().ReadMessage(gomock.Any()).Return(&input.msg, nil))
		msgToDelay[key{partition: input.msg.TopicPartition.Partition, offset: int64(input.msg.TopicPartition.Offset)}] = input.processDelay
	}
	// consumer.ReadMessage may get called multiple times after messages have been exhausted (this setup is for that scenario)
	consumerCalls = append(consumerCalls, consumer.EXPECT().ReadMessage(gomock.Any()).Return(nil, nil).AnyTimes())
	// setup consumer so that it reads input.msg1, input.msg2....input.msgN
	gomock.InOrder(
		consumerCalls...,
	)

	consumer.EXPECT().StoreOffsets(gomock.Any()).MaxTimes(messageCount)

	processor := timeDelayProcessor{
		msgToDelay: msgToDelay,
	}

	l := NoopLogger{}

	r := KReader{
		tCommitMgr: newTopicCommitMgr(),
		consumer:   consumer,
		topicConfig: ConsumerTopicConfig{
			ReadTimeoutMillis: ptr(1),
		},
		logger: l,
	}

	wf := WorkFactory{
		logger: l,
		tp:     noop.TracerProvider{},
		p:      propagation.TraceContext{},
	}

	countMtx := sync.Mutex{}
	processCount := atomic.Int64{}
	// we need to be in a concurrent environment so that we can simulate work happening simultaneously and finishing in orders
	// different from how the messages are read from the topic.
	// Additionally, we'll specify a callback function which will update as messages are processed
	gopoolsize := 100
	work := wf.Create(ConsumerTopicConfig{}, &processor, Speedup(uint16(gopoolsize)), WithOnDone(func(_ context.Context, _ *Message, _ error) {
		countMtx.Lock()
		processCount.Add(1)
		countMtx.Unlock()
	}))
	work.reader = &r

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		work.Run(ctx, nil)
	}()

	// we want to wait until all our input messages have been processed.
	sig := make(chan struct{})
	go func() {
		for {
			if processCount.Load() == int64(len(inputs)) {
				sig <- struct{}{}
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// However, if there's a bug in our test (or code) this might never occur.
	// We'll use this select case to specify a timeout for our tests
	select {
	case <-time.After(time.Second * 10):
		cancel()
		require.Fail(t, "test did not complete in expected time")
	case <-sig:
		cancel()
		break
	}

	for partition := 0; partition < partitionCount; partition++ {
		c := r.tCommitMgr.get(topicName)
		require.Contains(t, c.partitionToInWork, int32(partition), "expect inwork message map to contain holder for visited partition")
		require.Empty(t, c.partitionToInWork[int32(partition)].data, "expect inwork message map to contain holder for visited partition")
		require.Contains(t, c.partitionToCompleted, int32(partition), "expect partitionToCompleted message map to contain holder for visited partition")
		require.Empty(t, c.partitionToInWork[int32(partition)].data, "expect inwork message map to contain holder for visited partition")
	}
}

// TestWork_WithDoneWithContext asserts that the context in the done
// callback matches the context of the processed message.
func TestWork_WithDoneWithContext(t *testing.T) {
	defer recoverThenFail(t)
	type testInput struct {
		// the message to be processed
		msg kafka.Message
		// a simulated process delay for the associated message
		processDelay time.Duration
	}

	// arrange many kafka messages with random amounts of processing delay
	var inputs []testInput
	messageCount := 100
	partitionCount := 3
	topicName := "topic-name"
	for i := 0; i < messageCount; i++ {
		randPartition := i % partitionCount
		offset := i
		randDelayMillis := rand.Intn(10)
		inputs = append(inputs, testInput{
			msg: kafka.Message{
				TopicPartition: kafka.TopicPartition{Partition: int32(randPartition), Offset: kafka.Offset(offset), Topic: &topicName},
				Timestamp:      time.Now(),
				Key:            []byte(strconv.Itoa(i)),
			},
			processDelay: time.Duration(randDelayMillis) * time.Millisecond,
		})
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	consumer := mock_confluent.NewMockKafkaConsumer(ctrl)
	consumer.EXPECT().SubscribeTopics(gomock.Any(), gomock.Any()).Times(1)
	var consumerCalls []*gomock.Call

	msgToDelay := make(map[key]time.Duration)
	for i := range inputs {
		input := inputs[i]
		consumerCalls = append(consumerCalls, consumer.EXPECT().ReadMessage(gomock.Any()).Return(&input.msg, nil))
		msgToDelay[key{partition: input.msg.TopicPartition.Partition, offset: int64(input.msg.TopicPartition.Offset)}] = input.processDelay
	}
	// consumer.ReadMessage may get called multiple times after messages have been exhausted (this setup is for that scenario)
	consumerCalls = append(consumerCalls, consumer.EXPECT().ReadMessage(gomock.Any()).Return(nil, nil).AnyTimes())
	// setup consumer so that it reads input.msg1, input.msg2....input.msgN
	gomock.InOrder(
		consumerCalls...,
	)

	consumer.EXPECT().StoreOffsets(gomock.Any()).MaxTimes(messageCount)

	processor := timeDelayProcessor{
		msgToDelay: msgToDelay,
	}

	r := KReader{
		tCommitMgr: newTopicCommitMgr(),
		consumer:   consumer,
		topicConfig: ConsumerTopicConfig{
			ReadTimeoutMillis: ptr(1),
		},
		logger: NoopLogger{},
	}

	wf := WorkFactory{
		logger: NoopLogger{},
		tp:     noop.TracerProvider{},
		p:      propagation.TraceContext{},
	}

	countMtx := sync.Mutex{}
	processCount := atomic.Int64{}

	// Keep an array of the message keys seen (message keys were chosen to be string representation of ints)
	// Check that all have been seen after all messages are processed.
	msgsSeen := map[string]bool{}

	// we need to be in a concurrent environment so that we can simulate work happening simultaneously and finishing in orders
	// different from how the messages are read from the topic.
	// Additionally, we'll specify a callback function which will update as messages are processed
	gopoolsize := 100
	work := wf.Create(ConsumerTopicConfig{}, &processor, Speedup(uint16(gopoolsize)), WithOnDone(func(ctx context.Context, msg *Message, _ error) {
		countMtx.Lock()
		processCount.Add(1)
		msgsSeen[msg.Key] = true // Mark this context as seen
		countMtx.Unlock()
	}))
	work.reader = &r

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		work.Run(ctx, nil)
	}()

	// we want to wait until all our input messages have been processed.
	for {
		if processCount.Load() == int64(len(inputs)) {
			break
		}
		time.Sleep(time.Microsecond * 100)
	}

	// Make sure all messages were processed
	for _, i := range inputs {
		require.Contains(t, msgsSeen, string(i.msg.Key), "msg.Key not marked as seen (indicating it was not processed)")
	}
}

func Test_busyLoopBreaker_waitRespectsMaxPause(t *testing.T) {
	defer recoverThenFail(t)
	blb := busyLoopBreaker{
		mtx:      sync.Mutex{},
		maxPause: time.Microsecond,
	}
	// if this doesn't respect maxPause it would pause here indefinitely
	blb.wait()
}

// Test_busyLoopBreaker_waitRespectsRelease asserts that calling release() cancels that wait occuring at the wait() site
func Test_busyLoopBreaker_waitRespectsRelease(t *testing.T) {
	defer recoverThenFail(t)
	blb := busyLoopBreaker{
		mtx:      sync.Mutex{},
		maxPause: time.Second * 100,
	}

	// call blb.Wait() and only once it relinquishes that wait will we signal that its finished.
	// This signal can be used versus a timeout to assert
	blbFinishedWait := make(chan struct{})
	go func() {
		blb.wait()
		blbFinishedWait <- struct{}{}
	}()

	// wait a moment, so we can approximately guarantee that blb.wait has been called
	time.Sleep(time.Millisecond * 100)
	blb.release()

	select {
	case <-time.After(time.Second * 5):
		t.Fatalf("Test reached timeout of 5 seconds. blb.Release() method didnt return from blb.wait() site")
	case <-blbFinishedWait:
		break
	}
}

func Test_ShouldNotCircuitBreak(t *testing.T) {
	type testCase struct {
		err                   error
		shouldNotCircuitBreak bool
		description           string
	}

	tests := []testCase{
		{
			err:                   nil,
			shouldNotCircuitBreak: true,
			description:           "nil error should not trigger circuit breaker",
		},
		{
			err:                   errors.New("foobar"),
			shouldNotCircuitBreak: false,
			description:           "generic error should trigger circuit breaker",
		},
		{
			err: ProcessError{
				Err:                 errors.New("foobar"),
				DisableCircuitBreak: true,
			},
			shouldNotCircuitBreak: true,
			description:           "processerror with circuit break flag set should not trigger circuit breaker",
		},
		{
			err: ProcessError{
				Err:                 errors.New("foobar"),
				DisableCircuitBreak: false,
			},
			shouldNotCircuitBreak: false,
			description:           "processerror with circuit break flag NOT set should trigger circuit breaker",
		},
		{
			err: processorError{
				inner: errors.New("foobar"),
			},
			shouldNotCircuitBreak: false,
			description:           "workererror defaults to circuit break",
		},
		{
			err: processorError{
				inner: ProcessError{
					Err:                 errors.New("foobar"),
					DisableCircuitBreak: true,
				},
			},
			shouldNotCircuitBreak: true,
			description:           "workererror is properly unwrapped to check for processerror flag",
		},
		{
			err: processorError{
				inner: ProcessError{
					Err:                 errors.New("foobar"),
					DisableCircuitBreak: false,
				},
			},
			shouldNotCircuitBreak: false,
			description:           "workererror is properly unwrapped to check for processerror flag",
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			defer recoverThenFail(t)
			require.Equalf(t, test.shouldNotCircuitBreak, shouldNotCircuitBreak(test.err), test.description)
		})
	}
}

// Test_selectPartitionIndex_SelectsDifferentForDifferentInputsSometimes tests that the selectPartitionIndex function
// will return different values for different inputs sometimes. This is a probabilistic test, so it's possible that
// there are no collisions in 10 runs, but the probability of that is (1/10)^10, so it's unlikely.
// This test is meant to catch any obvious issues with the implementation (always returning the same index).
func Test_selectPartitionIndex_SelectsDifferentForDifferentInputsSometimes(t *testing.T) {
	for i := 0; i < 10; i++ {
		str1 := uuid.NewString()
		str2 := uuid.NewString()
		index1, err := selectPartitionIndex(str1, false, 10)
		require.NoError(t, err)

		index2, err := selectPartitionIndex(str2, false, 10)
		require.NoError(t, err)
		// break early, because we got generated different indexes for different inputs
		if index1 != index2 {
			return
		}
	}
	t.Fatal(t, "10 executions of SelectIndex were run which, for a proper implementation of SelectIndex has a (1/10)^10 chance of generating collisions each time."+
		" Likely there's an issue with the implementation")
}

func Test_selectPartitionIndex_SelectsDifferentForEmptyStringWithNilKeySometimes(t *testing.T) {
	for i := 0; i < 10; i++ {
		index1, err := selectPartitionIndex("", true, 10)
		require.NoError(t, err)

		index2, err := selectPartitionIndex("", true, 10)
		require.NoError(t, err)
		// break early, because we got generated different indexes for different inputs
		if index1 != index2 {
			return
		}
	}
	t.Fatal("10 executions of SelectIndex with empty key marked as nil were run which, for a proper implementation of SelectIndex has a (1/10)^10 chance of generating collisions each time." +
		" Likely there's an issue with the implementation")
}

func Test_selectPartitionIndex_SelectsSamePartitionWithEmptyKey(t *testing.T) {
	for i := 0; i < 10; i++ {
		index1, err := selectPartitionIndex("", false, 10)
		require.NoError(t, err)

		index2, err := selectPartitionIndex("", false, 10)
		require.NoError(t, err)

		require.Equal(t, index1, index2, "Expected that an empty string key always selects the same partition")
	}
}

func Test_calcDelay(t *testing.T) {
	now := time.Now()
	dc := delayCalculator{getNow: func() time.Time { return now }}

	type testCase struct {
		name            string
		configuredDelay time.Duration
		msgTimestamp    time.Time
		expectedDelay   time.Duration
	}

	testCases := []testCase{
		{
			name:            "timely-message-pays-full-delay-penalty",
			configuredDelay: time.Hour,
			msgTimestamp:    now,
			expectedDelay:   time.Hour,
		},
		{
			name:            "message-delayed-by-1-second-pays-delay-penalty-less-that-already-incurred-penalty",
			configuredDelay: 5 * time.Second,
			msgTimestamp:    now.Add(-1 * time.Second),
			expectedDelay:   4 * time.Second,
		},
		{
			name:            "message-delayed-by-2-second-pays-delay-penalty-less-that-already-incurred-penalty",
			configuredDelay: 5 * time.Second,
			msgTimestamp:    now.Add(-2 * time.Second),
			expectedDelay:   3 * time.Second,
		},
		{
			name:            "message-delayed-by-4-second-pays-delay-penalty-less-that-already-incurred-penalty",
			configuredDelay: 5 * time.Second,
			msgTimestamp:    now.Add(-4 * time.Second),
			expectedDelay:   1 * time.Second,
		},
		{
			name:            "message-delayed-by-configureDela-second-pays-no-delay-penalty",
			configuredDelay: 5 * time.Second,
			msgTimestamp:    now.Add(-5 * time.Second),
			expectedDelay:   0 * time.Second,
		},
		{
			name:            "future-messages-dont-incur-additional-delay",
			configuredDelay: 5 * time.Second,
			msgTimestamp:    now.Add(1 * time.Second),
			expectedDelay:   5 * time.Second,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			delay := dc.remaining(tc.configuredDelay, tc.msgTimestamp)
			require.Equal(t, delay, tc.expectedDelay)
		})
	}
}

func Fuzz_selectPartitionIndexAlwaysReturnsErrorWhenPartitionCountIsNotPositive(f *testing.F) {
	f.Add("hello", -3)
	f.Fuzz(func(t *testing.T, s string, max int) {
		if max > 0 {
			t.Skip()
		}
		_, err := selectPartitionIndex(s, false, max)
		require.Error(t, err)
	})
}

func Fuzz_selectPartitionIndexNeverReturnsErrorWhenPartitionCountIsPositive(f *testing.F) {
	f.Add("hello", 9)
	f.Fuzz(func(t *testing.T, s string, max int) {
		if max < 1 {
			t.Skip()
		}
		index, err := selectPartitionIndex(s, false, max)
		require.NoError(t, err)
		require.Less(t, index, max)
	})
}

func Fuzz_selectPartitionIndexReturnsSameIndexForSameString(f *testing.F) {
	f.Add("hello")
	f.Fuzz(func(t *testing.T, s string) {
		index1, err := selectPartitionIndex(s, false, 10)
		require.NoError(t, err)
		index2, err := selectPartitionIndex(s, false, 10)
		require.NoError(t, err)
		require.Equal(t, index1, index2, "Selected index should be the same for the same string")
	})
}

// Fuzz_AnySpeedupInputAlwaysCreatesABufferedChannel tests that the speedup parameter always creates a buffered channel
// for messageBuffer and virtualParititions
// messageBuffer is used to limit the number of outstanding messages that can be read from kafka which haven't been processed
func Fuzz_AnySpeedupInputAlwaysCreatesABufferedChannel(f *testing.F) {
	f.Add(uint16(9))

	f.Fuzz(func(t *testing.T, speedup uint16) {
		wf := NewWorkFactory(mockClientProvider{})
		p := timeDelayProcessor{}
		w := wf.Create(ConsumerTopicConfig{}, &p, Speedup(speedup))
		require.Greater(t, cap(w.messageBuffer), 0)
	})
}

type stdLogger struct{}

func (l stdLogger) Debugw(_ context.Context, msg string, keysAndValues ...any) {
	// log.Printf("Debugw-"+msg, keysAndValues...)
}

func (l stdLogger) Infow(_ context.Context, msg string, keysAndValues ...any) {
	log.Printf("Infow-"+msg, keysAndValues...)
}

func (l stdLogger) Errorw(_ context.Context, msg string, keysAndValues ...any) {
	log.Printf("Errorw-"+msg, keysAndValues...)
}

func (l stdLogger) Warnw(_ context.Context, msg string, keysAndValues ...any) {
	log.Printf("Warnw-"+msg, keysAndValues...)
}

type key struct {
	partition int32
	offset    int64
}

// timeDelayProcessor allows the simulation of processing delay on a per-message basis.
type timeDelayProcessor struct {
	// msgToDelay stores how long a particular messages simulated delay should be. It uses the offset and partition to identify the processDelay
	msgToDelay map[key]time.Duration
}

func (m *timeDelayProcessor) Process(_ context.Context, message *Message) error {
	timeDelay := m.msgToDelay[key{partition: message.Partition, offset: message.Offset}]
	time.Sleep(timeDelay)
	return nil
}

type mockClientProvider struct{}

func (mockClientProvider) Reader(ctx context.Context, topicConfig ConsumerTopicConfig, opts ...ReaderOption) (Reader, error) {
	return nil, nil
}

func (mockClientProvider) Writer(ctx context.Context, topicConfig ProducerTopicConfig, opts ...WriterOption) (Writer, error) {
	return nil, nil
}

func (mockClientProvider) Close() error {
	return nil
}

func assertContains(t *testing.T, wantIn kafka.TopicPartition, options []kafka.TopicPartition) {
	t.Helper()
	for _, want := range options {
		if wantIn == want {
			return
		}
	}
	msg := fmt.Sprintf("expected wantIn to appear in provided options\nwantIn: %s\noptions: %+v\n", wantIn, options)
	t.Fatal(msg)
}

type workSettings struct {
	shutdownSig chan struct{}
}

func (w *workSettings) ShutdownSig() <-chan struct{} {
	return w.shutdownSig
}
