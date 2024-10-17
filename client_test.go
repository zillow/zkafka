package zkafka

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
	"github.com/zillow/zfmt"
	mock_confluent "github.com/zillow/zkafka/mocks/confluent"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/mock/gomock"
)

func TestNewClient(t *testing.T) {
	type args struct {
		conf Config
	}
	tests := []struct {
		name string
		args args
		want *Client
	}{
		{
			name: "empty config",
			want: &Client{
				readers:          make(map[string]*KReader),
				writers:          make(map[string]*KWriter),
				logger:           NoopLogger{},
				producerProvider: defaultConfluentProducerProvider{}.NewProducer,
				consumerProvider: defaultConfluentConsumerProvider{}.NewConsumer,
			},
		},
		{
			name: "some config",
			args: args{
				conf: Config{
					BootstrapServers: []string{"test"},
				},
			},
			want: &Client{
				conf: Config{
					BootstrapServers: []string{"test"},
				},
				readers:          make(map[string]*KReader),
				writers:          make(map[string]*KWriter),
				logger:           NoopLogger{},
				producerProvider: defaultConfluentProducerProvider{}.NewProducer,
				consumerProvider: defaultConfluentConsumerProvider{}.NewConsumer,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer recoverThenFail(t)
			got := NewClient(tt.args.conf)
			require.Equal(t, tt.want.conf, got.conf)
			require.Equal(t, tt.want.readers, got.readers)
			require.Equal(t, tt.want.writers, got.writers)
			require.Equal(t, tt.want.logger, got.logger)
		})
	}
}

func TestClient_WithOptions(t *testing.T) {
	defer recoverThenFail(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	type fakeTracerProvider struct {
		trace.TracerProvider
		Salt int
	}
	type fakePropagator struct {
		propagation.TextMapPropagator
		Salt int
	}

	tp := fakeTracerProvider{Salt: 123}
	p := fakePropagator{Salt: 456}
	lprep := func(ctx context.Context, meta LifecyclePreProcessingMeta) (context.Context, error) {
		return ctx, errors.New("7")
	}
	lpostp := func(ctx context.Context, meta LifecyclePostProcessingMeta) error {
		return errors.New("8")
	}
	lposta := func(ctx context.Context, meta LifecyclePostAckMeta) error {
		return errors.New("9")
	}

	hooks := LifecycleHooks{
		PreProcessing:  lprep,
		PostProcessing: lpostp,
		PostAck:        lposta,
	}

	c := NewClient(Config{},
		LoggerOption(NoopLogger{}),
		WithClientTracerProviderOption(tp),
		WithClientTextMapPropagator(p),
		WithClientLifecycleHooks(hooks),
		KafkaGroupPrefixOption("servicename"))

	require.NotNil(t, c.logger, "WithLogger should set logger successfully")
	require.Equal(t, tp, c.tp)
	require.Equal(t, p, c.p)
	_, err := c.lifecycle.PreProcessing(context.Background(), LifecyclePreProcessingMeta{})
	require.Error(t, err, errors.New("7"))
	err = c.lifecycle.PostProcessing(context.Background(), LifecyclePostProcessingMeta{})
	require.Error(t, err, errors.New("8"))
	err = c.lifecycle.PostAck(context.Background(), LifecyclePostAckMeta{})
	require.Error(t, err, errors.New("9"))
	require.NotEmpty(t, c.groupPrefix, "group prefix should be set")
}

func TestClient_Reader(t *testing.T) {
	type fields struct {
		conf             Config
		readers          map[string]*KReader
		writers          map[string]*KWriter
		logger           Logger
		producerProvider confluentProducerProvider
		consumerProvider confluentConsumerProvider
	}
	type args struct {
		ctx         context.Context
		topicConfig ConsumerTopicConfig
		opts        []ReaderOption
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *KReader
		wantErr bool
	}{
		{
			name: "create new KReader with overridden Brokers, error from consumer provider",
			fields: fields{
				consumerProvider: mockConfluentConsumerProvider{err: true}.NewConsumer,
				readers:          make(map[string]*KReader),
			},
			args: args{
				topicConfig: ConsumerTopicConfig{
					ClientID:         "test-id",
					GroupID:          "group",
					Topic:            "topic",
					BootstrapServers: []string{"remotehost:8080"},
				},
			},
			wantErr: true,
		},
		{
			name: "create new KReader with bad formatter",
			fields: fields{
				consumerProvider: mockConfluentConsumerProvider{err: false}.NewConsumer,
				readers:          make(map[string]*KReader),
			},
			args: args{
				topicConfig: ConsumerTopicConfig{
					ClientID:         "test-id",
					GroupID:          "group",
					Topic:            "topic",
					Formatter:        zfmt.FormatterType("nonexistantformatter"),
					BootstrapServers: []string{"remotehost:8080"},
				},
			},
			wantErr: true,
		},
		{
			name: "create new KReader for closed KReader",
			fields: fields{
				conf: Config{BootstrapServers: []string{"localhost:9092"}},
				readers: map[string]*KReader{
					"test-config": {isClosed: true},
				},
				consumerProvider: mockConfluentConsumerProvider{c: MockKafkaConsumer{ID: "stew"}}.NewConsumer,
				logger:           NoopLogger{},
			},
			args: args{
				ctx:         context.TODO(),
				topicConfig: ConsumerTopicConfig{ClientID: "test-config", GroupID: "group", Topic: "topic"},
				opts:        []ReaderOption{RFormatterOption(&zfmt.AvroFormatter{})},
			},
			want: &KReader{
				consumer: MockKafkaConsumer{ID: "stew"},
				topicConfig: ConsumerTopicConfig{
					ClientID: "test-config",
					GroupID:  "group",
					Topic:    "topic",
					// some sensible default filled out by the client
					Formatter:             zfmt.JSONFmt,
					ReadTimeoutMillis:     ptr(1000),
					ProcessTimeoutMillis:  ptr(60000),
					SessionTimeoutMillis:  ptr(61000),
					MaxPollIntervalMillis: ptr(61000),
				},
				logger:    NoopLogger{},
				formatter: zfmtShim{&zfmt.AvroFormatter{}},
			},
			wantErr: false,
		},
		{
			name: "create new KReader for closed KReader with default overrides",
			fields: fields{
				conf: Config{BootstrapServers: []string{"localhost:9092"}},
				readers: map[string]*KReader{
					"test-config": {isClosed: true},
				},
				consumerProvider: mockConfluentConsumerProvider{c: MockKafkaConsumer{ID: "stew"}}.NewConsumer,
				logger:           NoopLogger{},
			},
			args: args{
				ctx:         context.TODO(),
				topicConfig: ConsumerTopicConfig{ClientID: "test-config", GroupID: "group", Topic: "topic", ReadTimeoutMillis: ptr(10000), ProcessTimeoutMillis: ptr(10000), SessionTimeoutMillis: ptr(20000), MaxPollIntervalMillis: ptr(21000)},
				opts:        []ReaderOption{RFormatterOption(&zfmt.AvroFormatter{})},
			},
			want: &KReader{
				consumer: MockKafkaConsumer{ID: "stew"},
				topicConfig: ConsumerTopicConfig{
					ClientID: "test-config",
					GroupID:  "group",
					Topic:    "topic",
					// some sensible default filled out by the client
					Formatter:             zfmt.JSONFmt,
					ReadTimeoutMillis:     ptr(10000),
					ProcessTimeoutMillis:  ptr(10000),
					SessionTimeoutMillis:  ptr(20000),
					MaxPollIntervalMillis: ptr(21000),
				},
				logger:    NoopLogger{},
				formatter: zfmtShim{&zfmt.AvroFormatter{}},
			},
			wantErr: false,
		},

		{
			name: "invalid configuration should return error",
			args: args{
				topicConfig: ConsumerTopicConfig{ClientID: "test"},
			},
			wantErr: true,
		},
		{
			name: "get from cache",
			fields: fields{
				readers: map[string]*KReader{
					"test-config": {},
				},
			},
			args: args{
				topicConfig: ConsumerTopicConfig{
					ClientID: "test-config",
					GroupID:  "group",
					Topic:    "topic",
				},
			},
			want:    &KReader{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer recoverThenFail(t)
			c := &Client{
				conf:    tt.fields.conf,
				readers: tt.fields.readers,
				writers: tt.fields.writers,
				logger:  tt.fields.logger,

				consumerProvider: tt.fields.consumerProvider,
				producerProvider: tt.fields.producerProvider,
			}
			got, err := c.Reader(tt.args.ctx, tt.args.topicConfig, tt.args.opts...)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			gotReader, ok := got.(*KReader)
			if err == nil && ok {

				assertEqual(t, gotReader.topicConfig, tt.want.topicConfig)
				a, aOk := tt.want.consumer.(MockKafkaConsumer)

				b, bOk := gotReader.consumer.(MockKafkaConsumer)
				if aOk && bOk {
					assertEqual(t, a, b, cmpopts.IgnoreUnexported(MockKafkaConsumer{}))
				}
				assertEqual(t, gotReader.logger, tt.want.logger)
				assertEqual(t, gotReader.formatter, tt.want.formatter)
			}
		})
	}
}

func TestClient_Writer(t *testing.T) {
	type fields struct {
		conf             Config
		readers          map[string]*KReader
		writers          map[string]*KWriter
		logger           Logger
		producerProvider confluentProducerProvider
	}
	type args struct {
		ctx         context.Context
		topicConfig ProducerTopicConfig
		opts        []WriterOption
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *KWriter
		wantErr bool
	}{
		{
			name: "create new KWriter with overridden Brokers, error from producer provider",
			fields: fields{
				producerProvider: mockConfluentProducerProvider{err: true}.NewProducer,
				writers:          make(map[string]*KWriter),
				conf: Config{
					SaslUsername: ptr("test-user"),
					SaslPassword: ptr("test-password"),
				},
			},
			args: args{
				topicConfig: ProducerTopicConfig{
					ClientID:         "test-id",
					Topic:            "topic",
					BootstrapServers: []string{"remotehost:8080"},
				},
			},
			wantErr: true,
		},
		{
			name: "create new KWriter for closed writer",
			fields: fields{
				conf: Config{BootstrapServers: []string{"localhost:9092"}},
				writers: map[string]*KWriter{
					"test-id": {isClosed: true},
				},
				producerProvider: mockConfluentProducerProvider{}.NewProducer,
				logger:           NoopLogger{},
			},
			args: args{
				topicConfig: ProducerTopicConfig{ClientID: "test-id", Topic: "topic"},
				opts:        []WriterOption{WFormatterOption(&zfmt.ProtobufRawFormatter{})},
			},
			want: &KWriter{
				topicConfig: ProducerTopicConfig{
					ClientID: "test-id",
					Topic:    "topic",
					// some sensible default filled out by the client
					Formatter:    zfmt.JSONFmt,
					NagleDisable: ptr(true),
					LingerMillis: 0,
				},
				logger:    NoopLogger{},
				tracer:    noop.TracerProvider{}.Tracer(""),
				p:         propagation.TraceContext{},
				formatter: zfmtShim{&zfmt.ProtobufRawFormatter{}},
			},
			wantErr: false,
		},
		{
			name: "create new KWriter for closed writer with default overrides",
			fields: fields{
				conf: Config{BootstrapServers: []string{"localhost:9092"}},
				writers: map[string]*KWriter{
					"test-id": {isClosed: true},
				},
				producerProvider: mockConfluentProducerProvider{}.NewProducer,
				logger:           NoopLogger{},
			},
			args: args{
				topicConfig: ProducerTopicConfig{ClientID: "test-id", Topic: "topic", LingerMillis: 1, NagleDisable: ptr(false)},
				opts:        []WriterOption{WFormatterOption(&zfmt.ProtobufRawFormatter{})},
			},
			want: &KWriter{
				topicConfig: ProducerTopicConfig{
					ClientID: "test-id",
					Topic:    "topic",
					// some sensible default filled out by the client
					Formatter:    zfmt.JSONFmt,
					NagleDisable: ptr(false),
					LingerMillis: 1,
				},
				logger:    NoopLogger{},
				tracer:    noop.TracerProvider{}.Tracer(""),
				p:         propagation.TraceContext{},
				formatter: zfmtShim{&zfmt.ProtobufRawFormatter{}},
			},
			wantErr: false,
		},
		{
			name: "invalid configuration should return error",
			args: args{
				topicConfig: ProducerTopicConfig{Topic: "topic"},
			},
			wantErr: true,
		},
		{
			name: "get from cache",
			fields: fields{
				writers: map[string]*KWriter{
					"test-id-topic": {},
				},
			},
			args: args{
				topicConfig: ProducerTopicConfig{ClientID: "test-id", Topic: "topic"},
			},
			want:    &KWriter{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer recoverThenFail(t)
			c := &Client{
				conf:             tt.fields.conf,
				readers:          tt.fields.readers,
				writers:          tt.fields.writers,
				logger:           tt.fields.logger,
				producerProvider: tt.fields.producerProvider,
			}
			got, err := c.Writer(tt.args.ctx, tt.args.topicConfig, tt.args.opts...)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if got == nil && tt.want == nil {
				return
			}
			gotKWriter := got.(*KWriter)
			require.NotNil(t, gotKWriter)

			assertEqual(t, gotKWriter.topicConfig, tt.want.topicConfig)
			assertEqual(t, gotKWriter.logger, tt.want.logger)
			assertEqual(t, gotKWriter.formatter, tt.want.formatter)
		})
	}
}

func TestClient_Close(t *testing.T) {
	p := MockKafkaProducer{ID: "d"}
	mockConsumer := MockKafkaConsumer{ID: "a", errClose: errors.New("error")}
	type fields struct {
		Mutex   *sync.Mutex
		conf    Config
		readers map[string]*KReader
		writers map[string]*KWriter
	}

	m := mockConfluentConsumerProvider{
		c: mockConsumer,
	}.NewConsumer
	r1, err := newReader(readerArgs{
		cfg: Config{BootstrapServers: []string{"localhost:9092"}},
		cCfg: ConsumerTopicConfig{
			Formatter: zfmt.StringFmt,
		},
		consumerProvider: m,
		f:                zfmtShim{F: &zfmt.StringFormatter{}},
		l:                &NoopLogger{},
	})
	require.NoError(t, err)
	r2, err := newReader(readerArgs{
		cfg: Config{BootstrapServers: []string{"localhost:9092"}},
		cCfg: ConsumerTopicConfig{
			Formatter: zfmt.StringFmt,
		},
		consumerProvider: m,
		f:                zfmtShim{F: &zfmt.StringFormatter{}},
		l:                &NoopLogger{},
	})
	require.NoError(t, err)
	tests := []struct {
		name    string
		wantErr bool
		fields  fields
	}{
		{
			name:   "no readers/writers => no error",
			fields: fields{},
		},
		{
			name:    "with readers/writers => no error",
			wantErr: true,
			fields: fields{
				readers: map[string]*KReader{
					"r1": r1,
					"r2": r2,
				},
				writers: map[string]*KWriter{
					"w1": {producer: p},
					"w2": {producer: p},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer recoverThenFail(t)
			c := &Client{
				conf:    tt.fields.conf,
				readers: tt.fields.readers,
				writers: tt.fields.writers,
			}
			err := c.Close()
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			for _, w := range c.writers {
				require.True(t, w.isClosed, "clients writer should be closed")
			}
			for _, r := range c.readers {
				require.True(t, r.isClosed, "clients reader should be closed")
			}
		})
	}
}

func Test_getFormatter_Consumer(t *testing.T) {
	type args struct {
		topicConfig ConsumerTopicConfig
	}
	tests := []struct {
		name    string
		args    args
		want    zfmt.Formatter
		wantErr bool
	}{
		{
			name:    "unsupported empty",
			args:    args{topicConfig: ConsumerTopicConfig{}},
			wantErr: true,
		},
		{
			name:    "string",
			args:    args{topicConfig: ConsumerTopicConfig{Formatter: zfmt.FormatterType("string")}},
			want:    &zfmt.StringFormatter{},
			wantErr: false,
		},
		{
			name:    "json",
			args:    args{topicConfig: ConsumerTopicConfig{Formatter: zfmt.FormatterType("json")}},
			want:    &zfmt.JSONFormatter{},
			wantErr: false,
		},
		{
			name:    "protocol buffer",
			args:    args{topicConfig: ConsumerTopicConfig{Formatter: zfmt.FormatterType("proto_raw")}},
			want:    &zfmt.ProtobufRawFormatter{},
			wantErr: false,
		},
		{
			name:    "apache avro",
			args:    args{topicConfig: ConsumerTopicConfig{Formatter: zfmt.FormatterType("avro")}},
			want:    &zfmt.AvroFormatter{},
			wantErr: false,
		},
		{
			name:    "confluent avro with inferred schema ClientID",
			args:    args{topicConfig: ConsumerTopicConfig{Formatter: zfmt.FormatterType("avro_schema"), SchemaID: 10}},
			want:    &zfmt.SchematizedAvroFormatter{SchemaID: 10},
			wantErr: false,
		},
		{
			name:    "confluent json with schema ID",
			args:    args{topicConfig: ConsumerTopicConfig{Formatter: zfmt.FormatterType("json_schema")}},
			want:    &zfmt.SchematizedJSONFormatter{},
			wantErr: false,
		},
		{
			name:    "confluent json with inferred schema ID",
			args:    args{topicConfig: ConsumerTopicConfig{Formatter: zfmt.FormatterType("json_schema"), SchemaID: 10}},
			want:    &zfmt.SchematizedJSONFormatter{SchemaID: 10},
			wantErr: false,
		},
		{
			name:    "confluent json with schema ID",
			args:    args{topicConfig: ConsumerTopicConfig{Formatter: zfmt.FormatterType("proto_schema_deprecated")}},
			want:    &zfmt.SchematizedProtoFormatterDeprecated{},
			wantErr: false,
		},
		{
			name:    "unsupported",
			args:    args{topicConfig: ConsumerTopicConfig{Formatter: zfmt.FormatterType("what"), SchemaID: 10}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer recoverThenFail(t)
			args := formatterArgs{
				formatter: tt.args.topicConfig.Formatter,
				schemaID:  tt.args.topicConfig.SchemaID,
			}
			c := Client{}
			got, err := c.getFormatter(args)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, zfmtShim{tt.want}, got)
			}
		})
	}
}

func Test_getFormatter_Producer(t *testing.T) {
	type args struct {
		topicConfig ProducerTopicConfig
	}
	tests := []struct {
		name    string
		args    args
		want    zfmt.Formatter
		wantErr bool
	}{
		{
			name:    "confluent avro with schema ClientID",
			args:    args{topicConfig: ProducerTopicConfig{Formatter: zfmt.FormatterType("avro_schema")}},
			want:    &zfmt.SchematizedAvroFormatter{},
			wantErr: false,
		},
		{
			name:    "confluent json with inferred schema ID",
			args:    args{topicConfig: ProducerTopicConfig{Formatter: zfmt.FormatterType("proto_schema_deprecated"), SchemaID: 10}},
			want:    &zfmt.SchematizedProtoFormatterDeprecated{SchemaID: 10},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer recoverThenFail(t)
			args := formatterArgs{
				formatter: tt.args.topicConfig.Formatter,
				schemaID:  tt.args.topicConfig.SchemaID,
			}
			c := Client{}
			got, err := c.getFormatter(args)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, zfmtShim{tt.want}, got)
			}
		})
	}
}

func TestClient_ConsumerProviderOptionAllowsForInjectionOfCustomConsumer(t *testing.T) {
	defer recoverThenFail(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg := Config{}
	ccfg := ConsumerTopicConfig{
		ClientID:         "test-id",
		GroupID:          "group",
		Topic:            "topic",
		BootstrapServers: []string{"remotehost:8080"},
	}
	mockConsumer := mock_confluent.NewMockKafkaConsumer(ctrl)
	mockConsumer.EXPECT().SubscribeTopics(gomock.Any(), gomock.Any()).MaxTimes(1)
	mockConsumer.EXPECT().ReadMessage(gomock.Any()).Return(&kafka.Message{
		Key: []byte("hello"),
	}, nil).AnyTimes()
	mockConsumer.EXPECT().Close().MaxTimes(1)

	c := NewClient(cfg,
		WithConsumerProvider(func(config map[string]any) (KafkaConsumer, error) {
			return mockConfluentConsumerProvider{
				c: mockConsumer,
			}.NewConsumer(nil)
		}),
	)

	reader, err := c.Reader(context.Background(), ccfg)
	require.NoError(t, err)

	msg, err := reader.Read(context.Background())
	require.NoError(t, err)
	require.Equal(t, "hello", msg.Key)
}

type mockConfluentConsumerProvider struct {
	c   KafkaConsumer
	err bool
}

func (p mockConfluentConsumerProvider) NewConsumer(_ kafka.ConfigMap) (KafkaConsumer, error) {
	err := errors.New("fake error")
	if !p.err {
		err = nil
	}
	return p.c, err
}

type mockConfluentProducerProvider struct {
	c   KafkaProducer
	err bool
}

func (p mockConfluentProducerProvider) NewProducer(_ kafka.ConfigMap) (KafkaProducer, error) {
	err := errors.New("fake error")
	if !p.err {
		err = nil
	}
	return p.c, err
}

var _ KafkaConsumer = (*MockKafkaConsumer)(nil)

type MockKafkaConsumer struct {
	ID       string
	err      error
	errClose error
}

func (m MockKafkaConsumer) SubscribeTopics(_ []string, _ kafka.RebalanceCb) error {
	return m.err
}

func (MockKafkaConsumer) Commit() ([]kafka.TopicPartition, error) {
	return nil, nil
}

func (MockKafkaConsumer) StoreOffsets(_ []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
	return nil, nil
}

func (m MockKafkaConsumer) ReadMessage(_ time.Duration) (*kafka.Message, error) {
	return nil, nil
}

func (m MockKafkaConsumer) Close() error {
	return m.errClose
}

func (m MockKafkaConsumer) Assignment() (partitions []kafka.TopicPartition, err error) {
	return nil, nil
}

func (m MockKafkaConsumer) Seek(partition kafka.TopicPartition, timeoutMs int) error {
	return nil
}

func (m MockKafkaConsumer) AssignmentLost() bool {
	return false
}

type MockKafkaProducer struct {
	ID  string
	err error
}

func (m MockKafkaProducer) Produce(_ *kafka.Message, _ chan kafka.Event) error {
	return m.err
}

func (m MockKafkaProducer) Close() {
}

func Test_makeConfig_Consumer(t *testing.T) {
	type args struct {
		conf        Config
		topicConfig ConsumerTopicConfig
		prefix      string
	}
	tests := []struct {
		name    string
		args    args
		want    kafka.ConfigMap
		wantErr string
	}{
		{
			name: "missing bootstrap",
			args: args{
				conf: Config{},
				topicConfig: ConsumerTopicConfig{
					ClientID:    "clientid",
					GroupID:     "group",
					Topic:       "",
					Formatter:   "",
					SchemaID:    0,
					Transaction: true,
				},
			},
			wantErr: "invalid consumer config, missing bootstrap server addresses",
		},

		{
			name: "with transaction",
			args: args{
				conf: Config{
					BootstrapServers: []string{"http://localhost:8080", "https://localhost:8081"},
				},
				topicConfig: ConsumerTopicConfig{
					ClientID:    "clientid",
					GroupID:     "group",
					Topic:       "",
					Formatter:   "",
					SchemaID:    0,
					Transaction: true,
				},
			},
			want: kafka.ConfigMap{
				"bootstrap.servers":        "http://localhost:8080,https://localhost:8081",
				"isolation.level":          "read_committed",
				"enable.auto.offset.store": false,
				"enable.auto.commit":       true,
				"group.id":                 "group",
				"client.id":                "clientid",
			},
		},
		{
			name: "with DeliveryTimeoutMs and AutoCommitIntervalMs",
			args: args{
				conf: Config{
					BootstrapServers: []string{"http://localhost:8080", "https://localhost:8081"},
				},
				topicConfig: ConsumerTopicConfig{
					ClientID:             "clientid",
					GroupID:              "group",
					AutoCommitIntervalMs: ptr(200),
				},
			},
			want: kafka.ConfigMap{
				"bootstrap.servers":        "http://localhost:8080,https://localhost:8081",
				"enable.auto.commit":       true,
				"enable.auto.offset.store": false,
				"group.id":                 "group",
				"client.id":                "clientid",
				"auto.commit.interval.ms":  200,
			},
		},
		{
			name: "with AdditionalProperties",
			args: args{
				conf: Config{
					BootstrapServers: []string{"http://localhost:8080"},
				},
				topicConfig: ConsumerTopicConfig{
					ClientID:             "clientid",
					GroupID:              "group",
					AutoCommitIntervalMs: ptr(200),
					AdditionalProps: map[string]any{
						"stewarts.random.property.not.included.in.topicconfig": 123,
					},
				},
			},
			want: kafka.ConfigMap{
				"bootstrap.servers":        "http://localhost:8080",
				"enable.auto.commit":       true,
				"enable.auto.offset.store": false,
				"group.id":                 "group",
				"client.id":                "clientid",
				"auto.commit.interval.ms":  200,
				"stewarts.random.property.not.included.in.topicconfig": 123,
			},
		},
		{
			name: "with AdditionalProperties that are floats (if not handled cause errors in confluent go)",
			args: args{
				conf: Config{
					BootstrapServers: []string{"http://localhost:8080"},
					SaslUsername:     ptr("username"),
					SaslPassword:     ptr("password"),
				},
				prefix: "xxxx",
				topicConfig: ConsumerTopicConfig{
					ClientID: "clientid",
					GroupID:  "group",
					AdditionalProps: map[string]any{
						"auto.commit.interval.ms": float32(20),
						"linger.ms":               float64(5),
					},
				},
			},
			want: kafka.ConfigMap{
				"bootstrap.servers":        "http://localhost:8080",
				"enable.auto.commit":       true,
				"enable.auto.offset.store": false,
				"group.id":                 "xxxx.group",
				"client.id":                "clientid",
				"auto.commit.interval.ms":  20,
				"linger.ms":                5,
				"sasl.mechanism":           "SCRAM-SHA-256",
				"sasl.password":            "password",
				"sasl.username":            "username",
				"security.protocol":        "SASL_SSL",
			},
		},
		{
			name: "with sasl override empty",
			args: args{
				conf: Config{
					BootstrapServers: []string{"http://localhost:8080"},
					SaslUsername:     ptr("username"),
					SaslPassword:     ptr("password"),
				},
				prefix: "xxxx",
				topicConfig: ConsumerTopicConfig{
					ClientID:     "clientid",
					GroupID:      "group",
					SaslUsername: ptr(""),
				},
			},
			want: kafka.ConfigMap{
				"bootstrap.servers":        "http://localhost:8080",
				"enable.auto.commit":       true,
				"enable.auto.offset.store": false,
				"group.id":                 "xxxx.group",
				"client.id":                "clientid",
			},
		},
		{
			name: "with sasl override override name",
			args: args{
				conf: Config{
					BootstrapServers: []string{"http://localhost:8080"},
					SaslUsername:     ptr("username"),
					SaslPassword:     ptr("password"),
				},
				prefix: "xxxx",
				topicConfig: ConsumerTopicConfig{
					ClientID:     "clientid",
					GroupID:      "group",
					SaslUsername: ptr("usernameOverride"),
				},
			},
			want: kafka.ConfigMap{
				"bootstrap.servers":        "http://localhost:8080",
				"enable.auto.commit":       true,
				"enable.auto.offset.store": false,
				"group.id":                 "xxxx.group",
				"client.id":                "clientid",
				"sasl.mechanism":           "SCRAM-SHA-256",
				"sasl.password":            "password",
				"sasl.username":            "usernameOverride",
				"security.protocol":        "SASL_SSL",
			},
		},
		{
			name: "with sasl override override pwd",
			args: args{
				conf: Config{
					BootstrapServers: []string{"http://localhost:8080"},
					SaslUsername:     ptr("username"),
					SaslPassword:     ptr("password"),
				},
				prefix: "xxxx",
				topicConfig: ConsumerTopicConfig{
					ClientID:     "clientid",
					GroupID:      "group",
					SaslPassword: ptr("passwordOverride"),
				},
			},
			want: kafka.ConfigMap{
				"bootstrap.servers":        "http://localhost:8080",
				"enable.auto.commit":       true,
				"enable.auto.offset.store": false,
				"group.id":                 "xxxx.group",
				"client.id":                "clientid",
				"sasl.mechanism":           "SCRAM-SHA-256",
				"sasl.password":            "passwordOverride",
				"sasl.username":            "username",
				"security.protocol":        "SASL_SSL",
			},
		},
		{
			name: "with sasl override override both",
			args: args{
				conf: Config{
					BootstrapServers: []string{"http://localhost:8080"},
					SaslUsername:     ptr("username"),
					SaslPassword:     ptr("password"),
				},
				prefix: "xxxx",
				topicConfig: ConsumerTopicConfig{
					ClientID:     "clientid",
					GroupID:      "group",
					SaslUsername: ptr("usernameOverride"),
					SaslPassword: ptr("passwordOverride"),
				},
			},
			want: kafka.ConfigMap{
				"bootstrap.servers":        "http://localhost:8080",
				"enable.auto.commit":       true,
				"enable.auto.offset.store": false,
				"group.id":                 "xxxx.group",
				"client.id":                "clientid",
				"sasl.mechanism":           "SCRAM-SHA-256",
				"sasl.password":            "passwordOverride",
				"sasl.username":            "usernameOverride",
				"security.protocol":        "SASL_SSL",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer recoverThenFail(t)
			got, err := makeConsumerConfig(tt.args.conf, tt.args.topicConfig, tt.args.prefix)
			if tt.wantErr == "" {
				require.NoError(t, err)
				assertEqual(t, got, tt.want)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func Test_makeConfig_Producer(t *testing.T) {
	type args struct {
		conf        Config
		topicConfig ProducerTopicConfig
	}
	tests := []struct {
		name    string
		args    args
		want    kafka.ConfigMap
		wantErr string
	}{
		{
			name: "with missing bootstrap config",
			args: args{
				conf: Config{},
				topicConfig: ProducerTopicConfig{
					ClientID:    "clientid",
					Topic:       "yyy",
					Transaction: true,
				},
			},
			wantErr: "invalid producer config, missing bootstrap server addresses",
		},

		{
			name: "with transaction",
			args: args{
				conf: Config{
					BootstrapServers: []string{"http://localhost:8080", "https://localhost:8081"},
				},
				topicConfig: ProducerTopicConfig{
					ClientID:    "clientid",
					Topic:       "yyy",
					Transaction: true,
				},
			},
			want: kafka.ConfigMap{
				"bootstrap.servers":                     "http://localhost:8080,https://localhost:8081",
				"enable.idempotence":                    true,
				"request.required.acks":                 -1,
				"max.in.flight.requests.per.connection": 1,
				"client.id":                             "clientid-yyy",
				"linger.ms":                             0,
			},
		},
		{
			name: "with DeliveryTimeoutMs and AutoCommitIntervalMs",
			args: args{
				conf: Config{
					BootstrapServers: []string{"http://localhost:8080", "https://localhost:8081"},
				},
				topicConfig: ProducerTopicConfig{
					ClientID:          "clientid",
					Topic:             "zzz",
					DeliveryTimeoutMs: ptr(100),
				},
			},
			want: kafka.ConfigMap{
				"bootstrap.servers":   "http://localhost:8080,https://localhost:8081",
				"client.id":           "clientid-zzz",
				"delivery.timeout.ms": 100,
				"enable.idempotence":  true,
				"linger.ms":           0,
			},
		},
		{
			name: "with AdditionalProperties",
			args: args{
				conf: Config{
					BootstrapServers: []string{"http://localhost:8080"},
				},
				topicConfig: ProducerTopicConfig{
					ClientID:          "clientid",
					Topic:             "zzz",
					DeliveryTimeoutMs: ptr(100),
					AdditionalProps: map[string]any{
						"stewarts.random.property.not.included.in.topicconfig": 123,
					},
				},
			},
			want: kafka.ConfigMap{
				"bootstrap.servers":   "http://localhost:8080",
				"enable.idempotence":  true,
				"client.id":           "clientid-zzz",
				"delivery.timeout.ms": 100,
				"stewarts.random.property.not.included.in.topicconfig": 123,
				"linger.ms": 0,
			},
		},
		{
			name: "with AdditionalProperties that are floats (if not handled cause errors in confluent go)",
			args: args{
				conf: Config{
					BootstrapServers: []string{"http://localhost:8080"},
					SaslUsername:     ptr("username"),
					SaslPassword:     ptr("password"),
				},
				topicConfig: ProducerTopicConfig{
					ClientID:            "clientid",
					Topic:               "abc",
					DeliveryTimeoutMs:   ptr(100),
					EnableIdempotence:   ptr(false),
					RequestRequiredAcks: ptr("all"),
					AdditionalProps: map[string]any{
						"auto.commit.interval.ms": float32(20),
						"linger.ms":               float64(5),
					},
				},
			},
			want: kafka.ConfigMap{
				"bootstrap.servers":       "http://localhost:8080",
				"client.id":               "clientid-abc",
				"enable.idempotence":      false,
				"delivery.timeout.ms":     100,
				"auto.commit.interval.ms": 20,
				"linger.ms":               5,
				"request.required.acks":   "all",
				"sasl.mechanism":          "SCRAM-SHA-256",
				"sasl.password":           "password",
				"sasl.username":           "username",
				"security.protocol":       "SASL_SSL",
			},
		},
		{
			name: "with sasl override empty",
			args: args{
				conf: Config{
					BootstrapServers: []string{"http://localhost:8080"},
					SaslUsername:     ptr("username"),
					SaslPassword:     ptr("password"),
				},
				topicConfig: ProducerTopicConfig{
					ClientID:     "clientid",
					Topic:        "xxx",
					SaslUsername: ptr(""),
				},
			},
			want: kafka.ConfigMap{
				"bootstrap.servers":  "http://localhost:8080",
				"client.id":          "clientid-xxx",
				"enable.idempotence": true,
				"linger.ms":          0,
			},
		},
		{
			name: "with sasl override override name",
			args: args{
				conf: Config{
					BootstrapServers: []string{"http://localhost:8080"},
					SaslUsername:     ptr("username"),
					SaslPassword:     ptr("password"),
				},
				topicConfig: ProducerTopicConfig{
					ClientID:     "clientid",
					Topic:        "xxx",
					SaslUsername: ptr("usernameOverride"),
				},
			},
			want: kafka.ConfigMap{
				"bootstrap.servers":  "http://localhost:8080",
				"client.id":          "clientid-xxx",
				"enable.idempotence": true,
				"sasl.mechanism":     "SCRAM-SHA-256",
				"sasl.password":      "password",
				"sasl.username":      "usernameOverride",
				"security.protocol":  "SASL_SSL",
				"linger.ms":          0,
			},
		},
		{
			name: "with sasl override override pwd",
			args: args{
				conf: Config{
					BootstrapServers: []string{"http://localhost:8080"},
					SaslUsername:     ptr("username"),
					SaslPassword:     ptr("password"),
				},
				topicConfig: ProducerTopicConfig{
					ClientID:     "clientid",
					Topic:        "xxx",
					SaslPassword: ptr("passwordOverride"),
				},
			},
			want: kafka.ConfigMap{
				"bootstrap.servers":  "http://localhost:8080",
				"client.id":          "clientid-xxx",
				"enable.idempotence": true,
				"sasl.mechanism":     "SCRAM-SHA-256",
				"sasl.password":      "passwordOverride",
				"sasl.username":      "username",
				"security.protocol":  "SASL_SSL",
				"linger.ms":          0,
			},
		},
		{
			name: "with sasl override override both",
			args: args{
				conf: Config{
					BootstrapServers: []string{"http://localhost:8080"},
					SaslUsername:     ptr("username"),
					SaslPassword:     ptr("password"),
				},
				topicConfig: ProducerTopicConfig{
					ClientID:     "clientid",
					Topic:        "xxx",
					SaslUsername: ptr("usernameOverride"),
					SaslPassword: ptr("passwordOverride"),
				},
			},
			want: kafka.ConfigMap{
				"bootstrap.servers":  "http://localhost:8080",
				"client.id":          "clientid-xxx",
				"enable.idempotence": true,
				"sasl.mechanism":     "SCRAM-SHA-256",
				"sasl.password":      "passwordOverride",
				"sasl.username":      "usernameOverride",
				"security.protocol":  "SASL_SSL",
				"linger.ms":          0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer recoverThenFail(t)
			got, err := makeProducerConfig(tt.args.conf, tt.args.topicConfig)
			if tt.wantErr == "" {
				require.NoError(t, err)
				assertEqual(t, got, tt.want)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func assertEqual(t *testing.T, got, want any, opts ...cmp.Option) {
	t.Helper()
	if diff := cmp.Diff(got, want, opts...); diff != "" {
		diff = fmt.Sprintf("\ngot: -\nwant: +\n%s", diff)
		t.Fatal(diff)
	}
}

func recoverThenFail(t *testing.T) {
	if r := recover(); r != nil {
		fmt.Print(string(debug.Stack()))
		t.Fatal(r)
	}
}
