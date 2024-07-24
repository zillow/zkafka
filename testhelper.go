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
func GetMsgFromFake(msg *FakeMessage) *Message {
	if msg == nil {
		return nil
	}
	key := ""
	if msg.Key != nil {
		key = *msg.Key
	}
	timeStamp := time.Now()
	if !msg.TimeStamp.IsZero() {
		timeStamp = msg.TimeStamp
	}
	doneFunc := func(ctx context.Context) {}
	if msg.DoneFunc != nil {
		doneFunc = msg.DoneFunc
	}
	var val []byte
	if msg.Value != nil {
		val = msg.Value
	}
	if msg.ValueData != nil {
		val, _ = msg.Fmt.Marshall(msg.ValueData)
	}
	return &Message{
		Key:       key,
		isKeyNil:  msg.Key == nil,
		Headers:   msg.Headers,
		Offset:    msg.Offset,
		Partition: msg.Partition,
		Topic:     msg.Topic,
		GroupID:   msg.GroupID,
		TimeStamp: timeStamp,
		value:     val,
		topicPartition: kafka.TopicPartition{
			Topic:     &msg.Topic,
			Partition: msg.Partition,
			Offset:    kafka.Offset(msg.Offset),
		},
		fmt:      msg.Fmt,
		doneFunc: doneFunc,
		doneOnce: sync.Once{},
	}
}
