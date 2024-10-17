package zkafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
	mock_confluent "github.com/zillow/zkafka/v2/mocks/confluent"

	"github.com/zillow/zfmt"
	"go.uber.org/mock/gomock"
)

func TestReader_Read_NilReturn(t *testing.T) {
	defer recoverThenFail(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockConsumer := mock_confluent.NewMockKafkaConsumer(ctrl)
	mockConsumer.EXPECT().SubscribeTopics(gomock.Any(), gomock.Any()).Times(1)
	mockConsumer.EXPECT().ReadMessage(gomock.Any()).Times(1)

	topicConfig := ConsumerTopicConfig{
		AutoCommitIntervalMs: ptr(10),
		ReadTimeoutMillis:    ptr(1000),
		Formatter:            zfmt.StringFmt,
	}
	m := mockConfluentConsumerProvider{
		c: mockConsumer,
	}.NewConsumer
	args := readerArgs{
		cfg:              Config{BootstrapServers: []string{"localhost:9092"}},
		cCfg:             topicConfig,
		consumerProvider: m,
		l:                &NoopLogger{},
	}
	r, err := newReader(args)
	require.NoError(t, err)

	got, err := r.Read(context.TODO())
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestReader_Read(t *testing.T) {
	defer recoverThenFail(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockConsumer := mock_confluent.NewMockKafkaConsumer(ctrl)
	mockConsumer.EXPECT().SubscribeTopics(gomock.Any(), gomock.Any()).Times(1)
	mockConsumer.EXPECT().ReadMessage(gomock.Any()).Return(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: ptr("topic"),
		},
		Value: []byte("test"),
	}, nil).Times(1)

	topicConfig := ConsumerTopicConfig{
		AutoCommitIntervalMs: ptr(10),
		ReadTimeoutMillis:    ptr(1000),
		Formatter:            zfmt.StringFmt,
	}
	m := mockConfluentConsumerProvider{
		c: mockConsumer,
	}.NewConsumer
	c := Client{}
	f, err := c.getFormatter(formatterArgs{formatter: topicConfig.Formatter})
	require.NoError(t, err)

	args := readerArgs{
		cfg: Config{BootstrapServers: []string{"localhost:9092"}}, cCfg: topicConfig,
		consumerProvider: m,
		l:                &NoopLogger{},
		f:                f,
	}
	r, err := newReader(args)
	require.NoError(t, err)

	got, err := r.Read(context.TODO())
	require.NoError(t, err)
	require.NotNil(t, got.fmt, "message should have formatter")
	require.NotEmpty(t, string(got.value), "expect a non-empty value on call to read")
}

func TestReader_Read_Error(t *testing.T) {
	readMessageErrors := []error{
		errors.New("error"),
		kafka.NewError(kafka.ErrAllBrokersDown, "error", true),
	}
	for _, readMessageError := range readMessageErrors {
		t.Run(readMessageError.Error(), func(t *testing.T) {
			defer recoverThenFail(t)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockConsumer := mock_confluent.NewMockKafkaConsumer(ctrl)
			mockConsumer.EXPECT().SubscribeTopics(gomock.Any(), gomock.Any()).Times(1)
			mockConsumer.EXPECT().ReadMessage(gomock.Any()).Return(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic: ptr("topic"),
				},
				Value: []byte("test"),
			}, nil).Return(nil, readMessageError).Times(1)

			topicConfig := ConsumerTopicConfig{
				Topic:                "topic",
				AutoCommitIntervalMs: ptr(10),
				ReadTimeoutMillis:    ptr(10),
				Formatter:            zfmt.StringFmt,
			}
			m := mockConfluentConsumerProvider{
				c: mockConsumer,
			}.NewConsumer
			args := readerArgs{
				cfg:              Config{BootstrapServers: []string{"localhost:9092"}},
				cCfg:             topicConfig,
				consumerProvider: m,
				l:                &NoopLogger{},
			}
			r, err := newReader(args)
			require.NoError(t, err)

			got, err := r.Read(context.TODO())
			require.Error(t, err)
			require.Nil(t, got)
		})
	}
}

func TestReader_Read_TimeoutError(t *testing.T) {
	defer recoverThenFail(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockConsumer := mock_confluent.NewMockKafkaConsumer(ctrl)
	mockConsumer.EXPECT().SubscribeTopics(gomock.Any(), gomock.Any()).Times(1)
	mockConsumer.EXPECT().ReadMessage(gomock.Any()).Return(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: ptr("topic"),
		},
		Value: []byte("test"),
	}, nil).Return(nil, kafka.NewError(kafka.ErrTimedOut, "error", false)).Times(1)

	topicConfig := ConsumerTopicConfig{
		Topic:                "topic",
		AutoCommitIntervalMs: ptr(10),
		Formatter:            zfmt.StringFmt,
		ReadTimeoutMillis:    ptr(1000),
	}
	m := mockConfluentConsumerProvider{
		c: mockConsumer,
	}.NewConsumer
	args := readerArgs{
		cfg:              Config{BootstrapServers: []string{"localhost:9092"}},
		cCfg:             topicConfig,
		consumerProvider: m,
		l:                &NoopLogger{},
	}
	r, err := newReader(args)
	require.NoError(t, err)

	got, err := r.Read(context.TODO())
	require.NoError(t, err, "expect no error to be returned on timeout")
	require.Nil(t, got, "expect a timeout to result in no read message")
}

func TestReader_Read_SubscriberError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockConsumer := mock_confluent.NewMockKafkaConsumer(ctrl)
	mockConsumer.EXPECT().SubscribeTopics(gomock.Any(), gomock.Any()).Return(errors.New("subscriber error")).Times(1)
	topicConfig := ConsumerTopicConfig{
		AutoCommitIntervalMs: ptr(10),
		Formatter:            zfmt.ProtoRawFmt,
	}
	m := mockConfluentConsumerProvider{
		c: mockConsumer,
	}.NewConsumer
	args := readerArgs{
		cfg: Config{BootstrapServers: []string{"localhost:9092"}}, cCfg: topicConfig,
		consumerProvider: m,
		l:                &NoopLogger{},
	}
	r, err := newReader(args)
	require.NoError(t, err)

	_, err = r.Read(context.TODO())
	require.Error(t, err, "expect an error to bubble up on Read because of subscribe error")
}

func TestReader_Read_CloseError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConsumer := mock_confluent.NewMockKafkaConsumer(ctrl)
	mockConsumer.EXPECT().Close().Return(errors.New("close error")).Times(1)
	l := mockLogger{}
	topicConfig := ConsumerTopicConfig{
		AutoCommitIntervalMs: ptr(10),
		Formatter:            zfmt.StringFmt,
	}
	m := mockConfluentConsumerProvider{
		c: mockConsumer,
	}.NewConsumer
	args := readerArgs{
		cfg: Config{BootstrapServers: []string{"localhost:9092"}}, cCfg: topicConfig,
		consumerProvider: m,
		l:                &l,
	}
	r, err := newReader(args)
	require.NoError(t, err)

	err = r.Close()
	require.Error(t, err)
}

func TestReader_ReadWhenConnectionIsClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockConsumer := mock_confluent.NewMockKafkaConsumer(ctrl)
	mockConsumer.EXPECT().SubscribeTopics(gomock.Any(), gomock.Any()).Times(1)
	mockConsumer.EXPECT().Close().Times(1)

	topicConfig := ConsumerTopicConfig{
		AutoCommitIntervalMs: ptr(10),
		Formatter:            zfmt.StringFmt,
	}
	m := mockConfluentConsumerProvider{
		c: mockConsumer,
	}.NewConsumer
	args := readerArgs{
		cfg: Config{BootstrapServers: []string{"localhost:9092"}}, cCfg: topicConfig,
		consumerProvider: m,
		l:                &NoopLogger{},
	}
	r, err := newReader(args)
	require.NoError(t, err)

	err = r.Close()
	require.NoError(t, err)
	_, err = r.Read(context.TODO())
	require.Error(t, err, "KReader.Read() message should return error due to connection lost")
}

func Test_newReader(t *testing.T) {
	type args struct {
		conf            Config
		topicConfig     ConsumerTopicConfig
		consumeProvider confluentConsumerProvider
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
				topicConfig: ConsumerTopicConfig{
					Formatter: zfmt.FormatterType("custom"),
				},
				consumeProvider: defaultConfluentConsumerProvider{}.NewConsumer,
			},
			wantErr: false,
		},
		{
			name: "valid formatter but has error when creating NewConsumer",
			args: args{
				consumeProvider: mockConfluentConsumerProvider{err: true}.NewConsumer,
			},
			wantErr: true,
		},
		{
			name: "minimum config with formatter",
			args: args{
				conf:            Config{BootstrapServers: []string{"localhost:9092"}},
				consumeProvider: defaultConfluentConsumerProvider{}.NewConsumer,
				topicConfig: ConsumerTopicConfig{
					Formatter: zfmt.StringFmt,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer recoverThenFail(t)
			args := readerArgs{
				cfg:              tt.args.conf,
				cCfg:             tt.args.topicConfig,
				consumerProvider: tt.args.consumeProvider,
				l:                &NoopLogger{},
			}
			_, err := newReader(args)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_ProcessMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dupMessage := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     ptr("test-topic"),
			Partition: 10,
			Offset:    99,
		},
	}

	l := mockLogger{}

	topicConfig := ConsumerTopicConfig{
		GroupID:              "test-group",
		AutoCommitIntervalMs: ptr(10),
		Formatter:            zfmt.StringFmt,
	}
	m := mockConfluentConsumerProvider{
		c: mock_confluent.NewMockKafkaConsumer(ctrl),
	}.NewConsumer
	args := readerArgs{
		cfg:              Config{BootstrapServers: []string{"localhost:9092"}},
		cCfg:             topicConfig,
		consumerProvider: m,
		l:                &NoopLogger{},
	}
	r, err := newReader(args)
	require.NoError(t, err)

	got := r.mapMessage(context.Background(), dupMessage)

	require.Equal(t, got.Partition, dupMessage.TopicPartition.Partition)
	require.Equal(t, got.Offset, int64(dupMessage.TopicPartition.Offset))
	require.Empty(t, got.Key)
	require.Empty(t, l.InfoStr, "no info calls should have been called")
}

func Test_ProcessMultipleMessagesFromDifferentTopics_UpdatesInternalStateProperly(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	topic1 := "test-topic1"
	topic2 := "test-topic2"
	msgs := []kafka.Message{
		{TopicPartition: kafka.TopicPartition{Topic: &topic1, Partition: 10, Offset: 99}},
		{TopicPartition: kafka.TopicPartition{Topic: &topic2, Partition: 5, Offset: 99}},
		{TopicPartition: kafka.TopicPartition{Topic: &topic2, Partition: 5, Offset: 100}},
	}

	l := mockLogger{}

	topicConfig := ConsumerTopicConfig{
		GroupID:              "test-group",
		AutoCommitIntervalMs: ptr(10),
		Formatter:            zfmt.StringFmt,
	}
	m := mockConfluentConsumerProvider{
		c: mock_confluent.NewMockKafkaConsumer(ctrl),
	}.NewConsumer

	args := readerArgs{
		cfg:              Config{BootstrapServers: []string{"localhost:9092"}},
		cCfg:             topicConfig,
		consumerProvider: m,
		l:                &l,
	}
	r, err := newReader(args)
	require.NoError(t, err)

	for _, msg := range msgs {
		got := r.mapMessage(context.Background(), msg)
		require.Equal(t, got.Partition, msg.TopicPartition.Partition)
		require.Equal(t, got.Offset, int64(msg.TopicPartition.Offset))
		require.Empty(t, got.Key)
	}

	expectedManagedTopicsCount := 2
	require.Len(t, r.tCommitMgr.topicToCommitMgr, expectedManagedTopicsCount, "expected to have 2 managed topics")
}

func Test_ProcessMessage_StoreOffsetError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	topicConfig := ConsumerTopicConfig{
		GroupID:              "test-group",
		Topic:                "test-topic",
		AutoCommitIntervalMs: ptr(10),
		Formatter:            zfmt.StringFmt,
	}

	dupMessage := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     ptr("test-topic"),
			Partition: 10,
			Offset:    99,
		},
	}

	l := mockLogger{}
	mockConsumer := mock_confluent.NewMockKafkaConsumer(ctrl)
	mockConsumer.EXPECT().StoreOffsets(gomock.Any()).DoAndReturn(func(m []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
		return nil, errors.New("error occurred on store")
	}).Times(1)

	m := mockConfluentConsumerProvider{
		c: mockConsumer,
	}.NewConsumer
	args := readerArgs{
		cfg:              Config{BootstrapServers: []string{"localhost:9092"}},
		cCfg:             topicConfig,
		consumerProvider: m,
		l:                &l,
	}
	r, err := newReader(args)
	require.NoError(t, err)

	mgr := newTopicCommitMgr()
	cmgr := mgr.get(*dupMessage.TopicPartition.Topic)
	cmgr.PushInWork(dupMessage.TopicPartition)
	r.tCommitMgr = mgr
	// assert that we won't block if Done on message is called
	msg := r.mapMessage(context.Background(), dupMessage)
	msg.Done()

	// if we can't store the offset, we expect a log to be set with no other errors raised
	require.Len(t, l.ErrorStr, 1, "expected an error log on fail to store offsets")
}

func Test_ProcessMessage_SetError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	topicConfig := ConsumerTopicConfig{
		GroupID:              "test-group",
		Topic:                "topic",
		AutoCommitIntervalMs: ptr(10),
		Formatter:            zfmt.StringFmt,
	}

	dupMessage := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     ptr(topicConfig.Topic),
			Partition: 10,
			Offset:    99,
		},
	}

	mockConsumer := mock_confluent.NewMockKafkaConsumer(ctrl)
	mockConsumer.EXPECT().StoreOffsets(gomock.Any()).DoAndReturn(func(m []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
		require.Len(t, m, 1, "expect one topicPartition to be passed")
		tp := m[0]
		require.Equal(t, dupMessage.TopicPartition.Partition, tp.Partition)
		require.Equal(t, dupMessage.TopicPartition.Offset+1, tp.Offset)
		return m, nil
	}).Times(1)

	l := mockLogger{}

	m := mockConfluentConsumerProvider{
		c: mockConsumer,
	}.NewConsumer
	args := readerArgs{
		cfg:              Config{BootstrapServers: []string{"localhost:9092"}},
		cCfg:             topicConfig,
		consumerProvider: m,
		l:                &l,
	}
	r, err := newReader(args)
	require.NoError(t, err)

	mgr := newTopicCommitMgr()
	cmgr := mgr.get(*dupMessage.TopicPartition.Topic)
	cmgr.PushInWork(dupMessage.TopicPartition)
	r.tCommitMgr = mgr
	msg := r.mapMessage(context.Background(), dupMessage)
	msg.Done()
}

func TestReader_CloseCalledMultipleTimesDoesntOnlyTriesToCloseKafkaConsumerOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	// setup mockKafkaConsumer to mimic behavior of real consumer (only being able to be closed once)
	mockKafkaConsumer1 := mock_confluent.NewMockKafkaConsumer(ctrl)
	gomock.InOrder(
		mockKafkaConsumer1.EXPECT().Close().Return(nil),
		mockKafkaConsumer1.EXPECT().Close().Return(errors.New("cant call twice")).AnyTimes(),
	)

	w := &KReader{
		consumer: mockKafkaConsumer1,
		logger:   NoopLogger{},
	}
	err := w.Close()
	require.NoError(t, err)

	err = w.Close()
	require.NoError(t, err)
}

func TestReader_Close(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockKafkaConsumer1 := mock_confluent.NewMockKafkaConsumer(ctrl)
	mockKafkaConsumer1.EXPECT().Close().Return(nil)
	mockKafkaConsumer2 := mock_confluent.NewMockKafkaConsumer(ctrl)
	mockKafkaConsumer2.EXPECT().Close().Return(errors.New("close error"))

	type fields struct {
		Mutex         *sync.Mutex
		kafkaConsumer KafkaConsumer
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "basic closure",
			fields: fields{
				kafkaConsumer: mockKafkaConsumer1,
			},
			wantErr: false,
		},
		{
			name: "basic closure with fake close error",
			fields: fields{
				kafkaConsumer: mockKafkaConsumer2,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		w := &KReader{
			consumer: tt.fields.kafkaConsumer,
			logger:   NoopLogger{},
		}
		err := w.Close()
		if tt.wantErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		require.True(t, w.isClosed, "KReader.Close() should have been closed")
	}
}

func TestKReader_getRebalanceCb_RevokedPartitionsWhenAssignedLostDoesntErrorDuringStandardExecution(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockKafkaConsumer1 := mock_confluent.NewMockKafkaConsumer(ctrl)
	mockKafkaConsumer1.EXPECT().AssignmentLost().Return(true)

	w := &KReader{
		consumer: mockKafkaConsumer1,
		logger:   NoopLogger{},
	}

	callback := w.getRebalanceCb()
	err := callback(nil, kafka.RevokedPartitions{})
	require.NoError(t, err)
}

func TestKReader_getRebalanceCb_RevokedPartitionsWhenAssignedNotLostDoesntErrorDuringStandardExecution(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockKafkaConsumer1 := mock_confluent.NewMockKafkaConsumer(ctrl)
	mockKafkaConsumer1.EXPECT().AssignmentLost().Return(false)

	w := &KReader{
		consumer: mockKafkaConsumer1,
		logger:   NoopLogger{},
	}

	callback := w.getRebalanceCb()
	err := callback(nil, kafka.RevokedPartitions{})
	require.NoError(t, err)
}

func TestKReader_getRebalanceCb_RecognizedKafkaErrorOccursDuringNonRebalanceCbEvent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockKafkaConsumer1 := mock_confluent.NewMockKafkaConsumer(ctrl)
	mockKafkaConsumer1.EXPECT().AssignmentLost().Return(false).Times(0)

	w := &KReader{
		consumer: mockKafkaConsumer1,
		logger:   NoopLogger{},
	}

	callback := w.getRebalanceCb()
	err := callback(nil, kafka.PartitionEOF{})
	require.NoError(t, err)
}

func Test_getTopics(t *testing.T) {
	type args struct {
		partitions []kafka.TopicPartition
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "ignore_nil_topic_names",
			args: args{
				[]kafka.TopicPartition{
					{
						Topic: nil,
					},
					{
						Topic: ptr("a"),
					},
					{
						Topic: ptr("b"),
					},
				},
			},
			want: []string{"a", "b"},
		},
		{
			name: "handle_nil_slice",
			args: args{
				partitions: nil,
			},
			want: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getTopics(tt.args.partitions)
			require.ElementsMatch(t, tt.want, got)
		})
	}
}

type mockLogger struct {
	ErrorStr  []string
	InfoStr   []string
	WarnStr   []string
	DPanicStr []string
}

func (m *mockLogger) Debugw(_ context.Context, _ string, _ ...any) {
}

func (m *mockLogger) Infow(_ context.Context, msg string, keysAndValues ...any) {
	m.InfoStr = append(m.InfoStr, fmt.Sprint(msg, keysAndValues))
}

func (m *mockLogger) Warnw(_ context.Context, msg string, keysAndValues ...any) {
	m.WarnStr = append(m.WarnStr, fmt.Sprint(msg, keysAndValues))
}

func (m *mockLogger) Errorw(_ context.Context, msg string, keysAndValues ...any) {
	m.ErrorStr = append(m.ErrorStr, fmt.Sprint(msg, keysAndValues))
}
