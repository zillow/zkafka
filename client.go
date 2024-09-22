package zkafka

//go:generate mockgen -package=mock_zkafka -destination=./mocks/mock_client.go -source=./client.go

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
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

	srf *schemaRegistryFactory

	producerProvider confluentProducerProvider
	consumerProvider confluentConsumerProvider
}

// NewClient instantiates a kafka client to get readers and writers
func NewClient(conf Config, opts ...Option) *Client {
	srf := newSchemaRegistryFactory()
	c := &Client{
		conf:    conf,
		readers: make(map[string]*KReader),
		writers: make(map[string]*KWriter),
		logger:  NoopLogger{},

		producerProvider: defaultConfluentProducerProvider{}.NewProducer,
		consumerProvider: defaultConfluentConsumerProvider{}.NewConsumer,
		srf:              srf,
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

	formatter, err := c.getFormatter(formatterArgs{
		formatter: topicConfig.Formatter,
		schemaID:  topicConfig.SchemaID,
		srCfg:     topicConfig.SchemaRegistry,
	})
	if err != nil {
		return nil, err
	}
	reader, err := newReader(readerArgs{
		cfg:              c.conf,
		cCfg:             topicConfig,
		consumerProvider: c.consumerProvider,
		f:                formatter,
		l:                c.logger,
		prefix:           c.groupPrefix,
		hooks:            c.lifecycle,
		opts:             opts,
	})
	if err != nil {
		return nil, err
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
	formatter, err := c.getFormatter(formatterArgs{
		formatter: topicConfig.Formatter,
		schemaID:  topicConfig.SchemaID,
		srCfg:     topicConfig.SchemaRegistry,
	})

	if err != nil {
		return nil, err
	}
	writer, err := newWriter(writerArgs{
		cfg:              c.conf,
		pCfg:             topicConfig,
		producerProvider: c.producerProvider,
		f:                formatter,
		l:                c.logger,
		t:                getTracer(c.tp),
		p:                c.p,
		hooks:            c.lifecycle,
		opts:             opts,
	})
	if err != nil {
		return nil, err
	}

	c.writers[topicConfig.ClientID] = writer
	return c.writers[topicConfig.ClientID], nil
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

func (c *Client) getFormatter(args formatterArgs) (kFormatter, error) {
	formatter := args.formatter
	schemaID := args.schemaID

	switch formatter {
	case AvroSchemaRegistry:
		scl, err := c.srf.createAvro(args.srCfg)
		if err != nil {
			return nil, err
		}
		cf, err := newAvroSchemaRegistryFormatter(scl)
		return cf, err
	case ProtoSchemaRegistry:
		scl, err := c.srf.createProto(args.srCfg)
		if err != nil {
			return nil, err
		}
		cf := newProtoSchemaRegistryFormatter(scl)
		return cf, nil
	case CustomFmt:
		return &errFormatter{}, nil
	default:
		f, err := zfmt.GetFormatter(formatter, schemaID)
		if err != nil {
			return nil, fmt.Errorf("unsupported formatter %s", formatter)
		}
		return zfmtShim{F: f}, nil
	}
}

type schemaRegistryFactory struct {
	mmu   sync.Mutex
	srCls map[string]schemaregistry.Client
}

func newSchemaRegistryFactory() *schemaRegistryFactory {
	return &schemaRegistryFactory{
		srCls: make(map[string]schemaregistry.Client),
	}
}

func (c *schemaRegistryFactory) createAvro(srConfig SchemaRegistryConfig) (avroFmt, error) {
	cl, err := c.getSchemaClient(srConfig)
	if err != nil {
		return avroFmt{}, err
	}

	deserConfig := avrov2.NewDeserializerConfig()
	deser, err := avrov2.NewDeserializer(cl, serde.ValueSerde, deserConfig)
	if err != nil {
		return avroFmt{}, fmt.Errorf("failed to create deserializer: %w", err)
	}

	serConfig := avrov2.NewSerializerConfig()
	serConfig.AutoRegisterSchemas = srConfig.Serialization.AutoRegisterSchemas
	serConfig.NormalizeSchemas = true

	ser, err := avrov2.NewSerializer(cl, serde.ValueSerde, serConfig)
	if err != nil {
		return avroFmt{}, fmt.Errorf("failed to create serializer: %w", err)
	}
	return avroFmt{
		ser:   ser,
		deser: deser,
	}, nil
}

func (c *schemaRegistryFactory) createProto(srConfig SchemaRegistryConfig) (protoFmt, error) {
	cl, err := c.getSchemaClient(srConfig)
	if err != nil {
		return protoFmt{}, err
	}

	deserConfig := protobuf.NewDeserializerConfig()
	deser, err := protobuf.NewDeserializer(cl, serde.ValueSerde, deserConfig)
	if err != nil {
		return protoFmt{}, fmt.Errorf("failed to create deserializer: %w", err)
	}

	serConfig := protobuf.NewSerializerConfig()
	serConfig.AutoRegisterSchemas = srConfig.Serialization.AutoRegisterSchemas
	serConfig.NormalizeSchemas = true

	ser, err := protobuf.NewSerializer(cl, serde.ValueSerde, serConfig)
	if err != nil {
		return protoFmt{}, fmt.Errorf("failed to create serializer: %w", err)
	}
	return protoFmt{
		ser:   ser,
		deser: deser,
	}, nil

}

func (c *schemaRegistryFactory) getSchemaClient(srConfig SchemaRegistryConfig) (schemaregistry.Client, error) {
	url := srConfig.URL
	if url == "" {
		return nil, errors.New("no schema registry url provided")
	}
	if srCl, ok := c.srCls[url]; ok {
		return srCl, nil
	}
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(url))
	if err != nil {
		return nil, fmt.Errorf("failed to create schema registry client: %w", err)
	}
	c.srCls[url] = client
	return client, nil
}

func getTracer(tp trace.TracerProvider) trace.Tracer {
	if tp == nil {
		return nil
	}
	return tp.Tracer(instrumentationName, trace.WithInstrumentationVersion("v1.0.0"))
}
