package zkafka

import (
	"context"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/zillow/zfmt"
)

// GetFakeMessage is a helper method for creating a *Message instance.
//
// Deprecated: As of v1.0.0, Prefer `GetMsgFromFake()`
func GetFakeMessage(key string, value any, fmt zfmt.Formatter, doneFunc func()) *Message {
	return getFakeMessage(key, value, fmt, doneFunc)
}

func getFakeMessage(key string, value any, fmt zfmt.Formatter, doneFunc func()) *Message {
	wrapperFunc := func(c context.Context) { doneFunc() }
	return GetMsgFromFake(&FakeMessage{
		Key:       &key,
		ValueData: value,
		Fmt:       fmt,
		DoneFunc:  wrapperFunc,
	})
}

// FakeMessage can be used during testing to construct Message objects.
// The Message object has private fields which might need to be tested
type FakeMessage struct {
	Key   *string
	Value []byte
	// ValueData allows the specification of serializable instance and uses the provided formatter
	// to create ValueData. Any error during serialization is ignored.
	ValueData any
	DoneFunc  func(ctx context.Context)
	Headers   map[string][]byte
	Offset    int64
	Partition int32
	Topic     string
	GroupID   string
	TimeStamp time.Time
	Fmt       zfmt.Formatter
}

// GetMsgFromFake allows the construction of a Message object (allowing the specification of some private fields).
func GetMsgFromFake(input *FakeMessage) *Message {
	if input == nil {
		return nil
	}
	key := ""
	if input.Key != nil {
		key = *input.Key
	}
	timeStamp := time.Now()
	if !input.TimeStamp.IsZero() {
		timeStamp = input.TimeStamp
	}
	doneFunc := func(ctx context.Context) {}
	if input.DoneFunc != nil {
		doneFunc = input.DoneFunc
	}
	var val []byte
	if input.Value != nil {
		val = input.Value
	}
	if input.ValueData != nil {
		//nolint:errcheck // To simplify this helper function's api, we'll suppress marshalling errors.
		val, _ = input.Fmt.Marshall(input.ValueData)
	}
	return &Message{
		Key:       key,
		isKeyNil:  input.Key == nil,
		Headers:   input.Headers,
		Offset:    input.Offset,
		Partition: input.Partition,
		Topic:     input.Topic,
		GroupID:   input.GroupID,
		TimeStamp: timeStamp,
		value:     val,
		topicPartition: kafka.TopicPartition{
			Topic:     &input.Topic,
			Partition: input.Partition,
			Offset:    kafka.Offset(input.Offset),
		},
		fmt:      zfmtShim{F: input.Fmt},
		doneFunc: doneFunc,
		doneOnce: sync.Once{},
	}
}

var _ ClientProvider = (*FakeClient)(nil)

// FakeClient is a convenience struct for testing purposes.
// It allows the specification of your own Reader/Writer while implementing the `ClientProvider` interface,
// which makes it compatible with a work factory.
type FakeClient struct {
	R Reader
	W Writer
}

func (f FakeClient) Reader(_ context.Context, _ ConsumerTopicConfig, _ ...ReaderOption) (Reader, error) {
	return f.R, nil
}

func (f FakeClient) Writer(_ context.Context, _ ProducerTopicConfig, _ ...WriterOption) (Writer, error) {
	return f.W, nil
}

func (f FakeClient) Close() error {
	return nil
}
