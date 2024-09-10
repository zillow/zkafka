package zkafka

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Message is a container for kafka message
type Message struct {
	Key string
	// There's a difference between a nil key and an empty key. A nil key gets assigned a topic partition by kafka via round-robin.
	// An empty key is treated as a key with a value of "" and is assigned to a topic partition via the hash of the key (so will consistently go to the same key)
	isKeyNil       bool
	Headers        map[string][]byte
	Offset         int64
	Partition      int32
	Topic          string
	GroupID        string
	TimeStamp      time.Time
	value          []byte
	topicPartition kafka.TopicPartition
	fmt            ultimateFormatter
	doneFunc       func(ctx context.Context)
	doneOnce       sync.Once
}

// DoneWithContext is used to alert that message processing has completed.
// This marks the message offset to be committed
func (m *Message) DoneWithContext(ctx context.Context) {
	m.doneOnce.Do(func() {
		m.doneFunc(ctx)
	})
}

// Done is used to alert that message processing has completed.
// This marks the message offset to be committed
func (m *Message) Done() {
	if m == nil {
		return
	}
	m.doneOnce.Do(func() {
		m.doneFunc(context.Background())
	})
}

// Decode reads message data and stores it in the value pointed to by v.
func (m *Message) Decode(v any) error {
	if m.value == nil {
		return errors.New("message is empty")
	}
	return m.unmarshall(v)
}

func (m *Message) unmarshall(target any) error {
	if m.fmt == nil {
		return errors.New("formatter or confluent formatter is not supplied to decode kafka message")
	}
	return m.fmt.Unmarshal(unmarshReq{
		topic:  m.Topic,
		data:   m.value,
		target: target,
	})
}

// Value returns a copy of the current value byte array. Useful for debugging
func (m *Message) Value() []byte {
	if m == nil || m.value == nil {
		return nil
	}
	out := make([]byte, len(m.value))
	copy(out, m.value)
	return out
}

func makeProducerMessageRaw(_ context.Context, topic string, key *string, value []byte) kafka.Message {
	kafkaMessage := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: value,
	}

	if key != nil {
		kafkaMessage.Key = []byte(*key)
	}
	return kafkaMessage
}

func addHeaders(kafkaMessage kafka.Message, headers map[string][]byte) kafka.Message {
	for k, v := range headers {
		addStringAttribute(&kafkaMessage, k, v)
	}
	return kafkaMessage
}

// addStringAttribute updates a kafka message header in place if the key exists already.
// If the key does not exist, it appends a new header.
func addStringAttribute(msg *kafka.Message, k string, v []byte) {
	for i, h := range msg.Headers {
		if h.Key == k {
			msg.Headers[i].Value = v
			return
		}
	}
	msg.Headers = append(msg.Headers, kafka.Header{Key: k, Value: v})
}

// Headers extracts metadata from kafka message and stores it in a basic map
func headers(msg kafka.Message) map[string][]byte {
	res := make(map[string][]byte)
	for _, h := range msg.Headers {
		res[h.Key] = h.Value
	}
	return res
}

// Response is a kafka response with the Partition where message was sent to along with its assigned Offset
type Response struct {
	Partition int32
	Offset    int64
}
