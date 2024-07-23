package zkafka

//go:generate mockgen -package=mock_zkafka -destination=./mocks/mock_client.go -source=./client.go

import (
	"context"
	"fmt"
	"sync"

	"github.com/zillow/zfmt"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ClientProvider is the convenient interface for kafka Client
type ClientProvider interface {
	Reader(ctx context.Context, topicConfig ConsumerTopicConfig, opts ...ReaderOption) (Reader, error)
	Writer(ctx context.Context, topicConfig ProducerTopicConfig, opts ...WriterOption) (Writer, error)
	Close() error
}

// static type checking for the convenient Writer interface
var _ ClientProvider = (*Client)(nil)

const instrumentationName = "github.com/zillow/zkafka"

// Client helps instantiate usable readers and writers
type Client struct {
	mu          sync.RWMutex
	conf        Config
	readers     map[string]*KReader
	writers     map[string]*KWriter
	logger      Logger
	lifecycle   LifecycleHooks
	groupPrefix string
	tp          trace.TracerProvider
	p           propagation.TextMapPropagator

	// confluent dependencies
	producerProvider confluentProducerProvider
	consumerProvider confluentConsumerProvider
}

// NewClient instantiates a kafka client to get readers and writers
func NewClient(conf Config, opts ...Option) *Client {
	c := &Client{
		conf:    conf,
		readers: make(map[string]*KReader),
		writers: make(map[string]*KWriter),
		logger:  NoopLogger{},

		producerProvider: defaultConfluentProducerProvider{}.NewProducer,
		consumerProvider: defaultConfluentConsumerProvider{}.NewConsumer,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Reader gets a kafka consumer from the provided config, either from cache or from a new instance
func (c *Client) Reader(_ context.Context, topicConfig ConsumerTopicConfig, opts ...ReaderOption) (Reader, error) {
	err := getDefaultConsumerTopicConfig(&topicConfig)
	if err != nil {
		return nil, err
	}
	c.mu.RLock()
	r, exist := c.readers[topicConfig.ClientID]
	if exist && !r.isClosed {
		c.mu.RUnlock()
		return r, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	r, exist = c.readers[topicConfig.ClientID]
	if exist && !r.isClosed {
		return r, nil
	}

	reader, err := newReader(c.conf, topicConfig, c.consumerProvider, c.logger, c.groupPrefix)
	if err != nil {
		return nil, err
	}
	// copy settings from client first
	reader.lifecycle = c.lifecycle

	// overwrite options if given
	for _, opt := range opts {
		opt(reader)
	}
	c.readers[topicConfig.ClientID] = reader
	return c.readers[topicConfig.ClientID], nil
}

// Writer gets a kafka producer from the provided config, either from cache or from a new instance
func (c *Client) Writer(_ context.Context, topicConfig ProducerTopicConfig, opts ...WriterOption) (Writer, error) {
	err := getDefaultProducerTopicConfig(&topicConfig)
	if err != nil {
		return nil, err
	}
	c.mu.RLock()
	w, exist := c.writers[topicConfig.ClientID]
	if exist && !w.isClosed {
		c.mu.RUnlock()
		return w, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	w, exist = c.writers[topicConfig.ClientID]
	if exist && !w.isClosed {
		return w, nil
	}
	writer, err := newWriter(c.conf, topicConfig, c.producerProvider)
	if err != nil {
		return nil, err
	}
	// copy settings from client first
	writer.logger = c.logger
	writer.tracer = getTracer(c.tp)
	writer.p = c.p
	writer.lifecycle = c.lifecycle

	// overwrite options if given
	for _, opt := range opts {
		opt(writer)
	}
	c.writers[topicConfig.ClientID] = writer
	return c.writers[topicConfig.ClientID], nil
}

func getFormatter(topicConfig TopicConfig) (zfmt.Formatter, error) {
	var fmtter zfmt.Formatter
	switch topicConfig.GetFormatter() {
	case CustomFmt:
		fmtter = &noopFormatter{}
	default:
		fmtter, _ = zfmt.GetFormatter(topicConfig.GetFormatter(), topicConfig.GetSchemaID())
	}
	if fmtter == nil {
		return nil, fmt.Errorf("unsupported formatter %s", topicConfig.GetFormatter())
	}
	return fmtter, nil
}

// Close terminates all cached readers and writers gracefully.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.logger != nil {
		c.logger.Debugw(context.Background(), "Close writers and readers")
	}

	var err error
	for _, w := range c.writers {
		w.Close()
	}
	for _, r := range c.readers {
		if e := r.Close(); e != nil {
			err = e
		}
	}
	return err
}

func getTracer(tp trace.TracerProvider) trace.Tracer {
	if tp == nil {
		return nil
	}
	return tp.Tracer(instrumentationName, trace.WithInstrumentationVersion("v1.0.0"))
}
