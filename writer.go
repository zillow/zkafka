package zkafka

//go:generate mockgen -package mock_confluent -destination=./mocks/confluent/kafka_producer.go . KafkaProducer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
	"go.opentelemetry.io/otel/trace"
)

// Writer is the convenient interface for kafka KWriter
type Writer interface {
	// Write sends messages to kafka with message key set as nil.
	// The value arg passed to this method is marshalled by
	// the configured formatter and used as the kafka message's value
	Write(ctx context.Context, value any, opts ...WriteOption) (Response, error)
	// WriteKey send message to kafka with a defined keys.
	// The value arg passed to this method is marshalled by
	// the configured formatter and used as the kafka message's value
	WriteKey(ctx context.Context, key string, value any, opts ...WriteOption) (Response, error)
	// WriteRaw sends messages to kafka. The caller is responsible for marshalling the data themselves.
	WriteRaw(ctx context.Context, key *string, value []byte, opts ...WriteOption) (Response, error)
	Close()
}

// static type checking for the convenient Writer interface
var _ Writer = (*KWriter)(nil)

type KafkaProducer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Close()
}

var _ KafkaProducer = (*kafka.Producer)(nil)

// KWriter is a kafka producer. KWriter should be initialized from the Client to be usable
type KWriter struct {
	mu          sync.Mutex
	producer    KafkaProducer
	topicConfig ProducerTopicConfig
	formatter   kFormatter
	logger      Logger
	tracer      trace.Tracer
	p           propagation.TextMapPropagator
	lifecycle   LifecycleHooks
	isClosed    bool
}

type keyValuePair struct {
	key   *string
	value any
}

type writerArgs struct {
	cfg              Config
	pCfg             ProducerTopicConfig
	producerProvider confluentProducerProvider
	f                kFormatter
	l                Logger
	t                trace.Tracer
	p                propagation.TextMapPropagator
	hooks            LifecycleHooks
	opts             []WriterOption
}

func newWriter(args writerArgs) (*KWriter, error) {
	conf := args.cfg
	topicConfig := args.pCfg
	producer := args.producerProvider
	formatter := args.f

	confluentConfig, err := makeProducerConfig(conf, topicConfig)
	if err != nil {
		return nil, err
	}

	p, err := producer(confluentConfig)
	if err != nil {
		return nil, err
	}

	w := &KWriter{
		producer:    p,
		topicConfig: topicConfig,
		formatter:   formatter,
		logger:      args.l,
		tracer:      args.t,
		p:           args.p,
		lifecycle:   args.hooks,
	}
	s := WriterSettings{}
	for _, opt := range args.opts {
		opt(&s)
	}
	if s.f != nil {
		w.formatter = s.f
	}
	return w, nil
}

// Write sends messages to kafka with message key set as nil.
// The value arg passed to this method is marshalled by
// the configured formatter and used as the kafka message's value
func (w *KWriter) Write(ctx context.Context, value any, opts ...WriteOption) (Response, error) {
	return w.write(ctx, keyValuePair{value: value}, opts...)
}

// WriteKey send message to kafka with a defined keys.
// The value arg passed to this method is marshalled by
// the configured formatter and used as the kafka message's value
func (w *KWriter) WriteKey(ctx context.Context, key string, value any, opts ...WriteOption) (Response, error) {
	return w.write(ctx, keyValuePair{
		key:   &key,
		value: value,
	}, opts...)
}

// WriteRaw allows you to write messages using a lower level API than Write and WriteKey.
// WriteRaw raw doesn't use a formatter to marshall the value data and instead takes the bytes as is and places them
// as the value for the kafka message
// It's convenient for forwarding message in dead letter operations.
func (w *KWriter) WriteRaw(ctx context.Context, key *string, value []byte, opts ...WriteOption) (Response, error) {
	kafkaMessage := makeProducerMessageRaw(ctx, w.topicConfig.Topic, key, value)
	for _, opt := range opts {
		opt.apply(&kafkaMessage)
	}
	if w.lifecycle.PreWrite != nil {
		resp, err := w.lifecycle.PreWrite(ctx, LifecyclePreWriteMeta{})
		if err != nil {
			w.logger.Warnw(ctx, "Lifecycle pre-write failed", "error", err)
		}
		kafkaMessage = addHeaders(kafkaMessage, resp.Headers)
	}

	w.logger.Debugw(ctx, "write message", "message", kafkaMessage)
	span := w.startSpan(ctx, &kafkaMessage)
	defer span.End()

	deliveryChan := make(chan kafka.Event)
	begin := time.Now()
	err := w.producer.Produce(&kafkaMessage, deliveryChan)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return Response{}, fmt.Errorf("error writing message: %w", err)
	}
	// wait on callback channel for kafka broker to ack written message
	e := <-deliveryChan

	w.lifecyclePostAck(ctx, begin)

	m, ok := e.(*kafka.Message)
	if !ok {
		return Response{}, errors.New("unexpected message delivered on kafka delivery channel")
	}

	span.SetAttributes(
		semconv.MessagingMessageIDKey.Int64(int64(m.TopicPartition.Offset)),
		semconv.MessagingKafkaDestinationPartitionKey.Int64(int64(m.TopicPartition.Partition)),
	)

	if m.TopicPartition.Error != nil {
		w.logger.Debugw(ctx, "Delivery failed", "error", m.TopicPartition.Error)
		return Response{}, fmt.Errorf("failed to produce kafka message: %w", m.TopicPartition.Error)
	}
	return Response{Partition: m.TopicPartition.Partition, Offset: int64(m.TopicPartition.Offset)}, nil
}

func (w *KWriter) lifecyclePostAck(ctx context.Context, begin time.Time) {
	if w.lifecycle.PostAck != nil {
		lcMeta := LifecyclePostAckMeta{
			Topic:       w.topicConfig.Topic,
			ProduceTime: begin,
		}

		if err := w.lifecycle.PostAck(ctx, lcMeta); err != nil {
			w.logger.Warnw(ctx, "Lifecycle post-ack failed", "error", err, "meta", lcMeta)
		}
	}
}

func (w *KWriter) startSpan(ctx context.Context, msg *kafka.Message) spanWrapper {
	if msg == nil || w.tracer == nil {
		return spanWrapper{}
	}
	topic := ""
	if msg.TopicPartition.Topic != nil {
		topic = *msg.TopicPartition.Topic
	}

	opts := []trace.SpanStartOption{
		trace.WithAttributes(
			semconv.MessagingOperationPublish,
			semconv.MessagingDestinationName(topic),
			semconv.MessagingKafkaMessageKey(string(msg.Key)),
			semconv.MessagingKafkaMessageOffset(int(msg.TopicPartition.Offset)),
			semconv.MessagingKafkaDestinationPartition(int(msg.TopicPartition.Partition)),
		),
		trace.WithSpanKind(trace.SpanKindProducer),
	}

	operationName := "zkafka.write"
	ctx, span := w.tracer.Start(ctx, operationName, opts...)

	// Inject the current span into the original message, so it can be used to
	// propagate the span.
	if w.p != nil {
		carrier := &kMsgCarrier{msg: msg}
		w.p.Inject(ctx, carrier)
	}
	return spanWrapper{span: span}
}

func (w *KWriter) write(ctx context.Context, msg keyValuePair, opts ...WriteOption) (Response, error) {
	value, err := w.marshall(ctx, msg.value, w.topicConfig.SchemaRegistry.Serialization.Schema)
	if err != nil {
		return Response{}, err
	}

	return w.WriteRaw(ctx, msg.key, value, opts...)
}

func (w *KWriter) marshall(_ context.Context, value any, schema string) ([]byte, error) {
	if w.formatter == nil {
		return nil, errors.New("formatter or confluent formatter is not supplied to produce kafka message")
	}
	return w.formatter.marshall(marshReq{
		topic:   w.topicConfig.Topic,
		subject: value,
		schema:  schema,
	})
}

// Close terminates the writer gracefully and mark it as closed
func (w *KWriter) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.producer.Close()
	w.isClosed = true
}

type WriterSettings struct {
	f kFormatter
}

// WriterOption is a function that modify the writer configurations
type WriterOption func(*WriterSettings)

// WFormatterOption sets the formatter for this writer
func WFormatterOption(f Formatter) WriterOption {
	return func(s *WriterSettings) {
		if f != nil {
			s.f = zfmtShim{F: f}
		}
	}
}

// WriteOption is a function that modifies the kafka.Message to be transmitted
type WriteOption interface {
	apply(s *kafka.Message)
}

// WithHeaders allows for the specification of headers. Specified headers will override collisions.
func WithHeaders(headers map[string]string) WriteOption { return withHeaderOption{headers: headers} }

type withHeaderOption struct {
	headers map[string]string
}

func (o withHeaderOption) apply(s *kafka.Message) {
	updateHeaders := func(k, v string) {
		header := kafka.Header{
			Key:   k,
			Value: []byte(v),
		}
		for i, h := range s.Headers {
			if h.Key == k {
				s.Headers[i] = header
				return
			}
		}
		s.Headers = append(s.Headers, header)
	}
	for k, v := range o.headers {
		updateHeaders(k, v)
	}
}
