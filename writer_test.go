package zkafka

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/zillow/zfmt"
	mock_confluent "github.com/zillow/zkafka/mocks/confluent"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestWriter_Write(t *testing.T) {
	recoverThenFail(t)

	ctrl := gomock.NewController(t)
	p := mock_confluent.NewMockKafkaProducer(ctrl)
	p.EXPECT().Produce(gomock.Any(), gomock.Any()).DoAndReturn(func(msg *kafka.Message, deliveryChan chan kafka.Event) error {
		go func() {
			deliveryChan <- &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Partition: 1,
					Offset:    1,
				},
			}
		}()
		return nil
	}).AnyTimes()

	type fields struct {
		Mutex    *sync.Mutex
		Producer KafkaProducer
		fmt      kFormatter
	}
	type args struct {
		ctx   context.Context
		value any
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    Response
		wantErr bool
	}{
		{
			name: "formatter check at minimum",
			fields: fields{
				fmt: nil,
			},
			args:    args{ctx: context.TODO(), value: "1"},
			want:    Response{Partition: 0, Offset: 0},
			wantErr: true,
		},
		{
			name: "has formatter and producer",
			fields: fields{
				fmt:      zfmtShim{&zfmt.StringFormatter{}},
				Producer: p,
			},
			args: args{ctx: context.TODO(), value: "1"},
			want: Response{Partition: 1, Offset: 1},
		},
		{
			name: "has formatter, producer, incompatible message type",
			fields: fields{
				fmt:      zfmtShim{&zfmt.StringFormatter{}},
				Producer: p,
			},
			args:    args{ctx: context.TODO(), value: 5},
			want:    Response{Partition: 1, Offset: 1},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer recoverThenFail(t)

			w := &KWriter{
				producer:  tt.fields.Producer,
				formatter: tt.fields.fmt,
				logger:    NoopLogger{},
				tracer:    noop.TracerProvider{}.Tracer(""),
				p:         propagation.TraceContext{},
			}
			got, err := w.Write(tt.args.ctx, tt.args.value)
			if tt.wantErr {
				require.Error(t, err, "expected error for KWriter.Write()")
			} else {
				require.Equal(t, tt.want, got, "expected response for KWriter.Write()")
			}
		})
	}
}

func TestWriter_WriteKey(t *testing.T) {
	recoverThenFail(t)
	ctrl := gomock.NewController(t)

	p := mock_confluent.NewMockKafkaProducer(ctrl)
	p.EXPECT().Produce(gomock.Any(), gomock.Any()).DoAndReturn(func(msg *kafka.Message, deliveryChan chan kafka.Event) error {
		go func() {
			deliveryChan <- &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Partition: 1,
					Offset:    1,
				},
			}
		}()
		return nil
	}).AnyTimes()

	contextWithSpan, _ := otel.GetTracerProvider().Tracer("").Start(context.Background(), "sdf")

	type fields struct {
		Mutex    *sync.Mutex
		Producer KafkaProducer
		conf     ProducerTopicConfig
		fmt      zfmt.Formatter
		isClosed bool
	}
	type args struct {
		ctx   context.Context
		key   string
		value any
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    Response
		wantErr bool
	}{
		{
			name: "valid keyValPairs",
			fields: fields{
				fmt:      &zfmt.StringFormatter{},
				Producer: p,
			},
			args:    args{ctx: context.TODO(), key: "key1", value: "msg1"},
			want:    Response{Partition: 1, Offset: 1},
			wantErr: false,
		},
		{
			name: "valid keyValPairs with partition spanning context",
			fields: fields{
				fmt:      &zfmt.StringFormatter{},
				Producer: p,
			},
			args:    args{ctx: contextWithSpan, key: "key1", value: "msg1"},
			want:    Response{Partition: 1, Offset: 1},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer recoverThenFail(t)
			w := &KWriter{
				producer:    tt.fields.Producer,
				topicConfig: tt.fields.conf,
				formatter:   zfmtShim{tt.fields.fmt},
				isClosed:    tt.fields.isClosed,
				logger:      NoopLogger{},
				tracer:      noop.TracerProvider{}.Tracer(""),
				p:           propagation.TraceContext{},
			}
			got, err := w.WriteKey(tt.args.ctx, tt.args.key, tt.args.value)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.want, got)
		})
	}
}

// TestWriter_WriteKeyReturnsImmediateError given a writer is used to try and produce a message
// when the producer returns an immediate error (as opposed) to on the channel
// then the error is bubbled up
//
// The producer doesn't typically return immediate errors (only if you're doing something obviously incorrect, librdkafka is too old for example).
// However, it was noticed, that there was a possible deadlock that occurred when the quick error was returned. This test assures that's
// no longer the behavior and the error is bubbled up
func TestWriter_WriteKeyReturnsImmediateError(t *testing.T) {
	recoverThenFail(t)
	ctrl := gomock.NewController(t)

	p := mock_confluent.NewMockKafkaProducer(ctrl)
	p.EXPECT().Produce(gomock.Any(), gomock.Any()).DoAndReturn(func(msg *kafka.Message, deliveryChan chan kafka.Event) error {
		return errors.New("not implemented error")
	}).AnyTimes()

	defer recoverThenFail(t)
	w := &KWriter{
		producer:    p,
		topicConfig: ProducerTopicConfig{},
		isClosed:    false,
		formatter:   zfmtShim{&zfmt.JSONFormatter{}},
		logger:      NoopLogger{},
		tracer:      noop.TracerProvider{}.Tracer(""),
		p:           propagation.TraceContext{},
	}
	_, err := w.WriteKey(context.Background(), "key", "val")
	require.Error(t, err)
}

func TestWriter_WritesMetrics(t *testing.T) {
	recoverThenFail(t)
	ctrl := gomock.NewController(t)

	p := mock_confluent.NewMockKafkaProducer(ctrl)
	p.EXPECT().Produce(gomock.Any(), gomock.Any()).DoAndReturn(func(msg *kafka.Message, deliveryChan chan kafka.Event) error {
		go func() {
			deliveryChan <- &kafka.Message{}
		}()
		return nil
	}).AnyTimes()

	th := testLifecycleHooks{}
	hooks := LifecycleHooks{
		PreProcessing: func(ctx context.Context, meta LifecyclePreProcessingMeta) (context.Context, error) {
			return th.PreProcessing(ctx, meta)
		},
		PostProcessing: func(ctx context.Context, meta LifecyclePostProcessingMeta) error {
			return th.PostProcessing(ctx, meta)
		},
		PostAck: func(ctx context.Context, meta LifecyclePostAckMeta) error {
			return th.PostAck(ctx, meta)
		},
	}

	wr := &KWriter{
		producer:    p,
		topicConfig: ProducerTopicConfig{Topic: "orange"},
		lifecycle:   hooks,
		formatter:   zfmtShim{&zfmt.StringFormatter{}},
		logger:      NoopLogger{},
		tracer:      noop.TracerProvider{}.Tracer(""),
		p:           propagation.TraceContext{},
	}

	_, err := wr.WriteKey(context.Background(), "apple", "mango")
	require.NoError(t, err)

	// Pre- and Post-Processing are covered in read tests.  We are testing writes here.
	require.Len(t, th.preProMeta, 0)
	require.Len(t, th.postProMeta, 0)
	require.Len(t, th.postAckMeta, 1)
	require.Equal(t, th.postAckMeta[0].Topic, "orange")
}

func TestWriter_WriteSpecialCase(t *testing.T) {
	recoverThenFail(t)

	ctrl := gomock.NewController(t)

	p1 := mock_confluent.NewMockKafkaProducer(ctrl)
	p1.EXPECT().Produce(gomock.Any(), gomock.Any()).DoAndReturn(func(msg *kafka.Message, deliveryChan chan kafka.Event) error {
		go func() {
			deliveryChan <- &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Error: errors.New("an error"),
				},
			}
		}()
		return errors.New("an error")
	}).AnyTimes()

	type fields struct {
		Mutex    *sync.Mutex
		Producer KafkaProducer
		fmt      zfmt.Formatter
	}
	type args struct {
		ctx   context.Context
		value any
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    Response
		wantErr bool
	}{
		{
			name: "partition message in the batch failed",
			fields: fields{
				fmt:      &zfmt.StringFormatter{},
				Producer: p1,
			},
			args:    args{ctx: context.TODO(), value: "mgs2"},
			want:    Response{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &KWriter{
				producer:  tt.fields.Producer,
				formatter: zfmtShim{tt.fields.fmt},
				logger:    NoopLogger{},
				tracer:    noop.TracerProvider{}.Tracer(""),
				p:         propagation.TraceContext{},
			}
			got, err := w.Write(tt.args.ctx, tt.args.value)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestWriter_PreWriteLifecycleHookCanAugmentHeaders(t *testing.T) {
	recoverThenFail(t)
	ctrl := gomock.NewController(t)

	p := mock_confluent.NewMockKafkaProducer(ctrl)
	var capturedMsg *kafka.Message
	p.EXPECT().Produce(gomock.Any(), gomock.Any()).DoAndReturn(func(msg *kafka.Message, deliveryChan chan kafka.Event) error {
		go func() {
			deliveryChan <- &kafka.Message{}
		}()
		capturedMsg = msg
		return nil
	}).AnyTimes()

	hooks := LifecycleHooks{
		PreWrite: func(ctx context.Context, meta LifecyclePreWriteMeta) (LifecyclePreWriteResp, error) {
			return LifecyclePreWriteResp{
				Headers: map[string][]byte{
					"hello": []byte("world"),
				},
			}, nil
		},
	}

	wr := &KWriter{
		producer:    p,
		topicConfig: ProducerTopicConfig{Topic: "orange"},
		lifecycle:   hooks,
		formatter:   zfmtShim{&zfmt.StringFormatter{}},
		logger:      NoopLogger{},
		tracer:      noop.TracerProvider{}.Tracer(""),
		p:           propagation.TraceContext{},
	}

	_, err := wr.WriteKey(context.Background(), "apple", "mango")
	require.NoError(t, err)

	require.Contains(t, capturedMsg.Headers, kafka.Header{Key: "hello", Value: []byte("world")})
}

func TestWriter_WithHeadersWriteOptionCanAugmentHeaders(t *testing.T) {
	recoverThenFail(t)
	ctrl := gomock.NewController(t)

	p := mock_confluent.NewMockKafkaProducer(ctrl)
	var capturedMsg *kafka.Message
	p.EXPECT().Produce(gomock.Any(), gomock.Any()).DoAndReturn(func(msg *kafka.Message, deliveryChan chan kafka.Event) error {
		go func() {
			deliveryChan <- &kafka.Message{}
		}()
		capturedMsg = msg
		return nil
	}).AnyTimes()

	wr := &KWriter{
		producer:    p,
		topicConfig: ProducerTopicConfig{Topic: "orange"},
		formatter:   zfmtShim{&zfmt.StringFormatter{}},
		logger:      NoopLogger{},
		tracer:      noop.TracerProvider{}.Tracer(""),
		p:           propagation.TraceContext{},
	}

	_, err := wr.WriteKey(context.Background(), "apple", "mango", WithHeaders(map[string]string{
		"hello": "world",
	}))
	require.NoError(t, err)

	require.Contains(t, capturedMsg.Headers, kafka.Header{Key: "hello", Value: []byte("world")})
}

func Test_WithHeadersUpdatesOnConflict(t *testing.T) {
	recoverThenFail(t)

	opt := WithHeaders(map[string]string{
		"abc": "def",
	})
	msg := &kafka.Message{
		Headers: []kafka.Header{
			{
				Key:   "abc",
				Value: []byte("xxx"),
			},
		},
	}
	opt.apply(msg)
	require.Len(t, msg.Headers, 1)
	require.Equal(t, msg.Headers[0], kafka.Header{
		Key:   "abc",
		Value: []byte("def"),
	})
}

func TestWriter_PreWriteLifecycleHookErrorDoesntHaltProcessing(t *testing.T) {
	recoverThenFail(t)
	ctrl := gomock.NewController(t)

	p := mock_confluent.NewMockKafkaProducer(ctrl)
	p.EXPECT().Produce(gomock.Any(), gomock.Any()).DoAndReturn(func(msg *kafka.Message, deliveryChan chan kafka.Event) error {
		go func() {
			deliveryChan <- &kafka.Message{}
		}()
		return nil
	}).AnyTimes()

	hooks := LifecycleHooks{
		PreWrite: func(ctx context.Context, meta LifecyclePreWriteMeta) (LifecyclePreWriteResp, error) {
			return LifecyclePreWriteResp{}, errors.New("pre write hook")
		},
	}

	wr := &KWriter{
		producer:    p,
		topicConfig: ProducerTopicConfig{Topic: "orange"},
		lifecycle:   hooks,
		formatter:   zfmtShim{&zfmt.StringFormatter{}},
		logger:      NoopLogger{},
		tracer:      noop.TracerProvider{}.Tracer(""),
		p:           propagation.TraceContext{},
	}

	_, err := wr.WriteKey(context.Background(), "apple", "mango")
	require.NoError(t, err)
}

func TestWriter_Close(t *testing.T) {
	recoverThenFail(t)
	ctrl := gomock.NewController(t)
	p1 := mock_confluent.NewMockKafkaProducer(ctrl)
	p1.EXPECT().Close().AnyTimes()
	p2 := mock_confluent.NewMockKafkaProducer(ctrl)
	p2.EXPECT().Close().AnyTimes()

	type fields struct {
		Mutex    *sync.Mutex
		Producer KafkaProducer
		isClosed bool
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "basic closure",
			fields: fields{
				Producer: p1,
			},
			wantErr: false,
		},
		{
			name: "basic closure with fake close error",
			fields: fields{
				Producer: p2,
			},
			wantErr: true,
		},
	}
	// No parallel test since Close() mocks are sequential
	for _, tt := range tests {
		w := &KWriter{
			producer: tt.fields.Producer,
			isClosed: tt.fields.isClosed,
			logger:   NoopLogger{},
			tracer:   noop.TracerProvider{}.Tracer(""),
			p:        propagation.TraceContext{},
		}
		w.Close()
		require.True(t, w.isClosed, "KWriter.Close() should have been closed")
	}
}

func Test_newWriter(t *testing.T) {
	type args struct {
		conf        Config
		topicConfig ProducerTopicConfig
		producerP   confluentProducerProvider
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "custom formatter, no error. It is implied that user will supply formatter later",
			args: args{
				conf: Config{BootstrapServers: []string{"localhost:9092"}},
				topicConfig: ProducerTopicConfig{
					Formatter: zfmt.FormatterType("custom"),
				},
				producerP: defaultConfluentProducerProvider{}.NewProducer,
			},
			wantErr: false,
		},
		//{
		//	name: "invalid formatter",
		//	args: args{
		//		producerP: defaultConfluentProducerProvider{}.NewProducer,
		//		topicConfig: ProducerTopicConfig{
		//			Formatter: zfmt.FormatterType("invalid_fmt"),
		//		},
		//	},
		//	wantErr: true,
		//},
		{
			name: "valid formatter but has error from confluent producer constructor",
			args: args{
				producerP: mockConfluentProducerProvider{err: true}.NewProducer,
			},
			wantErr: true,
		},
		{

			name: "minimum config with formatter",
			args: args{
				conf:      Config{BootstrapServers: []string{"localhost:9092"}},
				producerP: defaultConfluentProducerProvider{}.NewProducer,
				topicConfig: ProducerTopicConfig{
					Formatter: zfmt.StringFmt,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recoverThenFail(t)
			args := writerArgs{
				cfg:              tt.args.conf,
				pCfg:             tt.args.topicConfig,
				producerProvider: tt.args.producerP,
			}
			w, err := newWriter(args)
			if tt.wantErr {
				require.Error(t, err, "expected error for newWriter()")
			} else {
				require.NoError(t, err)
				require.NotNil(t, w, "worker should be initialized")
			}
		})
	}
}

func TestWriter_WithOptions(t *testing.T) {
	recoverThenFail(t)
	w := &KWriter{}
	require.Nil(t, w.formatter, "expected nil formatter")

	settings := WriterSettings{}
	WFormatterOption(&zfmt.StringFormatter{})(&settings)
	require.NotNil(t, settings.f, "expected non-nil formatter")
}

func Test_writeAttributeCarrier_Set(t *testing.T) {
	recoverThenFail(t)
	km := kafka.Message{}
	c := kMsgCarrier{
		msg: &km,
	}
	c.Set("hello", "world")
	expected := kafka.Header{
		Key:   "hello",
		Value: []byte("world"),
	}
	assertEqual(t, km.Headers[0], expected)
}

// testLifecycle ...
type testLifecycleHooks struct {
	preProMeta  []LifecyclePreProcessingMeta
	postProMeta []LifecyclePostProcessingMeta
	postAckMeta []LifecyclePostAckMeta
}

func (l *testLifecycleHooks) PreProcessing(ctx context.Context, meta LifecyclePreProcessingMeta) (context.Context, error) {
	l.preProMeta = append(l.preProMeta, meta)
	return ctx, nil
}

func (l *testLifecycleHooks) PostProcessing(ctx context.Context, meta LifecyclePostProcessingMeta) error {
	l.postProMeta = append(l.postProMeta, meta)
	return nil
}

func (l *testLifecycleHooks) PostAck(ctx context.Context, meta LifecyclePostAckMeta) error {
	l.postAckMeta = append(l.postAckMeta, meta)
	return nil
}
