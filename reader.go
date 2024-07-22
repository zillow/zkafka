package zstreams

//go:generate mockgen  -package mock_confluent  --destination=./mocks/confluent/kafka_consumer.go . KafkaConsumer
//go:generate mockgen  --package=mock_zstreams  --destination=./mocks/mock_reader.go  . Reader

import (
	"context"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pkg/errors"
)

//// go:generate mockgen -destination=./mocks/mock_metrics.go -source=reader.go

// Reader is the convenient interface for kafka KReader
type Reader interface {
	Read(ctx context.Context) (*Message, error)
	Close() error
}

// static type checking for the convenient Reader interface
var _ Reader = (*KReader)(nil)

type KafkaConsumer interface {
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error
	ReadMessage(timeout time.Duration) (*kafka.Message, error)
	Commit() ([]kafka.TopicPartition, error)
	StoreOffsets(offsets []kafka.TopicPartition) (storedOffsets []kafka.TopicPartition, err error)
	Close() error
	Assignment() (partitions []kafka.TopicPartition, err error)
	AssignmentLost() bool
}

var _ KafkaConsumer = (*kafka.Consumer)(nil)

// KReader is a Reader implementation which allows for the subscription to multiple topics. It provides methods
// for consuming messages from its subscribed topics and assigned partitions.
type KReader struct {
	consumer    KafkaConsumer
	topicConfig ConsumerTopicConfig
	isClosed    bool

	fmtter     Formatter
	logger     Logger
	lifecycle  LifecycleHooks
	once       sync.Once
	tCommitMgr *topicCommitMgr
}

// newReader makes a new reader based on the configurations
func newReader(conf Config, topicConfig ConsumerTopicConfig, provider confluentConsumerProvider, logger Logger, prefix string) (*KReader, error) {
	confluentConfig := makeConsumerConfig(conf, topicConfig, prefix)
	consumer, err := provider(confluentConfig)
	if err != nil {
		return nil, err
	}

	fmtter, err := getFormatter(topicConfig)
	if err != nil {
		return nil, err
	}
	return &KReader{
		consumer:    consumer,
		fmtter:      fmtter,
		topicConfig: topicConfig,
		logger:      logger,
		tCommitMgr:  newTopicCommitMgr(),
	}, nil
}

// Read consumes a single message at a time. Blocks until a message is returned or some
// non-fatal error occurs in which case a nil message is returned
func (r *KReader) Read(ctx context.Context) (*Message, error) {
	r.once.Do(func() {
		rebalanceCb := r.getRebalanceCb()
		err := r.consumer.SubscribeTopics(r.topicConfig.topics(), rebalanceCb)
		if err != nil {
			r.logger.Errorw(ctx, "Failed to subscribe to topic", "topics", r.topicConfig.topics(), "error", err)
			r.isClosed = true
		}
	})
	if r.isClosed {
		return nil, errors.New("reader closed")
	}
	kmsg, err := r.consumer.ReadMessage(time.Duration(*r.topicConfig.ReadTimeoutMillis) * time.Millisecond)
	if err != nil {
		switch v := err.(type) {
		case kafka.Error:
			// timeouts occur (because the assigned partitions aren't being written to, lack of activity, etc.). We'll
			// log them for debugging purposes
			if v.Code() == kafka.ErrTimedOut {
				r.logger.Debugw(ctx, "timed out on read", "topics", r.topicConfig.topics())
				return nil, nil
			}
			if v.IsRetriable() {
				r.logger.Debugw(ctx, "Retryable error occurred", "topics", r.topicConfig.topics(), "error", v)
				return nil, nil
			}
			return nil, errors.Wrap(err, "failed to read kafka message")
		}
		return nil, errors.Wrap(err, "failed to read kafka message")
	}
	if kmsg == nil {
		return nil, nil
	}
	return r.mapMessage(ctx, *kmsg), nil
}

// Close terminates the consumer. This will gracefully unsubscribe
// the consumer from the kafka topic (which includes properly
// revoking the assigned partitions)
func (r *KReader) Close() error {
	if r.isClosed {
		return nil
	}
	r.isClosed = true
	err := r.consumer.Close()
	if err != nil {
		return errors.Wrap(err, "failed to close kafka reader")
	}
	return nil
}

// Assignments returns the current partition assignments for the kafka consumer
func (r *KReader) Assignments(_ context.Context) ([]Assignment, error) {
	assignments, err := r.consumer.Assignment()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get assignments")
	}
	topicPartitions := make([]Assignment, 0, len(assignments))
	for _, tp := range assignments {
		if tp.Topic == nil {
			continue
		}
		topicPartitions = append(topicPartitions, Assignment{
			Partition: tp.Partition,
			Topic:     *tp.Topic,
		})
	}
	return topicPartitions, nil
}

func (r *KReader) removeInWork(offset kafka.TopicPartition) {
	topicName := getTopicName(offset.Topic)
	c := r.tCommitMgr.get(topicName)
	c.RemoveInWork(offset)
}

// mapMessage is responsible for mapping the confluent kafka.Message to a zstreams.Message.
func (r *KReader) mapMessage(_ context.Context, msg kafka.Message) *Message {
	headerMap := headers(msg)

	topicName := getTopicName(msg.TopicPartition.Topic)
	c := r.tCommitMgr.get(topicName)
	c.PushInWork(msg.TopicPartition)

	partition := msg.TopicPartition.Partition
	offset := int64(msg.TopicPartition.Offset)
	return &Message{
		Key:            string(msg.Key),
		isKeyNil:       msg.Key == nil,
		Headers:        headerMap,
		Partition:      partition,
		Topic:          topicName,
		GroupID:        r.topicConfig.GroupID,
		Offset:         offset,
		topicPartition: msg.TopicPartition,
		TimeStamp:      msg.Timestamp,
		doneFunc: func(ctx context.Context) {
			c.PushCompleted(msg.TopicPartition)
			commitOffset := c.TryPop(ctx, partition)
			if commitOffset == nil {
				r.logger.Debugw(ctx, "Message complete, but can't commit yet", "topicName", topicName, "groupID", r.topicConfig.GroupID, "partition", partition, "offset", offset)
				return
			}

			if commitOffset.Error != nil {
				r.logger.Errorw(ctx, "Message complete, but can't commit because of error", "commitOffset", commitOffset)
				return
			}

			if commitOffset.Offset < 0 {
				r.logger.Errorw(ctx, "Message complete, but can't commit because offset < 0", "commitOffset", commitOffset)
				return
			}

			// https://github.com/confluentinc/confluent-kafka-go/v2/blob/master/kafka/consumer.go#L297
			// https://github.com/confluentinc/confluent-kafka-go/v2/issues/656
			commitOffset.Offset++

			_, err := r.consumer.StoreOffsets([]kafka.TopicPartition{*commitOffset})
			r.logger.Debugw(ctx, "Stored offsets", "offset", commitOffset, "groupID", r.topicConfig.GroupID)
			if err != nil {
				r.logger.Errorw(ctx, "Error storing offsets", "topicName", topicName, "groupID", r.topicConfig.GroupID, "partition", partition, "offset", offset, "error", err)
			}
		},
		value: msg.Value,
		fmt:   r.fmtter,
	}
}

// getRebalanceCb returns a callback which can be used during rebalances.
// It previously attempted to do one final, explict commit of stored offsets.
// This was unncessary per the mantainer of librdkafka (https://github.com/confluentinc/librdkafka/issues/1829#issuecomment-393427324)
// since when using auto.offset.commit=true (which this library does) the offsets are commit at configured intervals, during close and finally during rebalance.
//
// We do however, want to attempt to let current work complete before allowing a rebalance (so we check the in progress heap) for up to 10 seconds.
//
// This is part of the commit management strategy per guidance here https://docs.confluent.io/platform/current/clients/consumer.html#offset-management
// commit when partitions are revoked
func (r *KReader) getRebalanceCb() kafka.RebalanceCb {
	ctx := context.Background()
	rebalanceCb := func(_ *kafka.Consumer, event kafka.Event) error {
		switch e := event.(type) {
		case kafka.AssignedPartitions:
			r.logger.Infow(ctx, "Assigned partitions event received", "event", e, "topics", r.topicConfig.topics(), "groupID", r.topicConfig.GroupID)
		case kafka.RevokedPartitions:
			r.logger.Infow(ctx, "Revoked partitions event received", "event", e, "topics", r.topicConfig.topics(), "groupID", r.topicConfig.GroupID)

			// Usually, the rebalance callback for `RevokedPartitions` is called
			// just before the partitions are revoked. We can be certain that a
			// partition being revoked is not yet owned by any other consumer.
			// This way, logic like storing any pending offsets or committing
			// offsets can be handled.
			// However, there can be cases where the assignment is lost
			// involuntarily. In this case, the partition might already be owned
			// by another consumer, and operations including committing
			// offsets may not work. (this part of the check comes from this confluent-kafka-go example)[https://github.com/confluentinc/confluent-kafka-go/blob/master/examples/consumer_rebalance_example/consumer_rebalance_example.go]
			if r.consumer.AssignmentLost() {
				r.logger.Infow(ctx, "Assignment lost prior to revoke (possibly because client was closed)", "event", e, "topics", r.topicConfig.topics(), "groupID", r.topicConfig.GroupID)
				return nil
			}

			// we're going to try and finish processing the inwork messages.
			// We'll do this by checking the commit manager. When the RevokedPartitions event is emitted,
			// subsequent ReadMessage() calls from a consumer will return nil, allowing the commit manager to get widdled down.
			ctxNew, cancel := context.WithTimeout(ctx, time.Second*10)
			defer cancel()
			for {
				var inWorkCount int64 = 0
				for _, t := range getTopics(e.Partitions) {
					cmtMgr := r.tCommitMgr.get(t)
					inWorkCount += cmtMgr.InWorkCount()
				}
				if inWorkCount == 0 {
					break
				}
				if ctxNew.Err() != nil {
					r.logger.Warnw(ctx, "Incomplete inwork drain during revokedPartitions event", "event", e, "inWorkCount", inWorkCount, "error", ctxNew.Err(), "topics", r.topicConfig.topics(), "groupID", r.topicConfig.GroupID)
					break
				}
				// we're polling the commit manager, we'll do a small pause to avoid a busy loop
				time.Sleep(time.Microsecond * 1)
			}
		}
		return nil
	}
	return rebalanceCb
}

func getTopics(partitions []kafka.TopicPartition) []string {
	uniqueTopics := map[string]struct{}{}
	for _, p := range partitions {
		if p.Topic == nil {
			continue
		}
		uniqueTopics[*p.Topic] = struct{}{}
	}
	topics := make([]string, 0, len(uniqueTopics))
	for t := range uniqueTopics {
		topics = append(topics, t)
	}
	return topics
}

func getTopicName(topicName *string) string {
	topic := ""
	if topicName != nil {
		topic = *topicName
	}
	return topic
}

// ReaderOption is a function that modify the KReader configurations
type ReaderOption func(*KReader)

// RFormatterOption sets the formatter for this reader
func RFormatterOption(fmtter Formatter) ReaderOption {
	return func(r *KReader) {
		if fmtter != nil {
			r.fmtter = fmtter
		}
	}
}

type TopicPartition struct {
	Partition int32
	Offset    int64
}

type Assignment struct {
	Partition int32
	Topic     string
}
