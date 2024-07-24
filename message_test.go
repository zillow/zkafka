package zkafka

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
	"github.com/zillow/zfmt"
)

func Test_makeProducerMessageRaw(t *testing.T) {
	type args struct {
		ctx         context.Context
		serviceName string
		topic       string
		key         *string
		value       []byte
	}
	tests := []struct {
		name       string
		args       args
		want       kafka.Message
		hasHeaders bool
	}{
		{
			name: "has fmtter with valid input, no key, no partition",
			args: args{
				serviceName: "concierge/test/test_group",
				topic:       "test_topic",
				value:       []byte("string"),
			},
			want: kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic: ptr("test_topic"),
					// this indicates any partition to confluent-kafka-go
					Partition: -1,
				},
				Opaque:  nil,
				Headers: nil,
			},
			hasHeaders: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer recoverThenFail(t)
			got := makeProducerMessageRaw(tt.args.ctx, tt.args.topic, tt.args.key, tt.args.value)
			require.Equal(t, tt.want.TopicPartition, got.TopicPartition)
			require.Equal(t, tt.want.Key, got.Key)
			require.Equal(t, tt.want.Key, got.Key)
			if tt.hasHeaders {
				require.NotEmpty(t, got.Headers)
			}
		})
	}
}

func TestMessage_Headers(t *testing.T) {
	type fields struct {
		msg kafka.Message
	}
	tests := []struct {
		name   string
		fields fields
		want   map[string][]byte
	}{
		{
			name:   "empty message",
			fields: fields{},
			want:   make(map[string][]byte),
		},
		{
			name: "msgs with headers",
			fields: fields{msg: kafka.Message{
				Headers: []kafka.Header{
					{
						Key:   "key1",
						Value: []byte("value1"),
					},
					{
						Key:   "key2",
						Value: []byte("value2"),
					},
				},
			}},
			want: map[string][]byte{
				"key1": []byte("value1"),
				"key2": []byte("value2"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer recoverThenFail(t)
			got := headers(tt.fields.msg)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMessage_Decode(t *testing.T) {
	type fields struct {
		value []byte
		fmt   zfmt.Formatter
	}
	type args struct {
		v any
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:    "empty message, empty input => error",
			fields:  fields{},
			args:    args{},
			wantErr: true,
		},
		{
			name: "valid message, no formatter, empty input => error",
			fields: fields{
				value: []byte("test"),
			},
			args:    args{},
			wantErr: true,
		},
		{
			name: "valid message, formatter, empty input => error",
			fields: fields{
				value: []byte("test"),
				fmt:   &zfmt.StringFormatter{},
			},
			args:    args{},
			wantErr: true,
		},
		{
			name: "valid message, formatter, valid input => no error",
			fields: fields{
				value: []byte("test"),
				fmt:   &zfmt.StringFormatter{},
			},
			args: args{
				v: &bytes.Buffer{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer recoverThenFail(t)
			m := Message{
				value: tt.fields.value,
				fmt:   tt.fields.fmt,
			}
			err := m.Decode(tt.args.v)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMessage_Done(t *testing.T) {
	type fields struct {
		Key     string
		Headers map[string][]byte
		value   []byte
		fmt     zfmt.Formatter
		doneSig chan bool
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "multiple calls to done",
			fields: fields{
				doneSig: make(chan bool, 1),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer recoverThenFail(t)
			isCalled := false
			m := &Message{
				Key:     tt.fields.Key,
				Headers: tt.fields.Headers,
				value:   tt.fields.value,
				fmt:     tt.fields.fmt,
				doneFunc: func(ctx context.Context) {
					isCalled = true
				},
			}
			// call done multiple times and function should still return
			var wg sync.WaitGroup
			wg.Add(3)
			for i := 0; i < 3; i++ {
				go func() {
					defer wg.Done()
					m.DoneWithContext(context.Background())
				}()
			}
			wg.Wait()
			require.True(t, isCalled, "doneFunc should have been called at least once")
		})
	}
}

func Test_addHeaders(t *testing.T) {
	type args struct {
		kafkaMessage kafka.Message
		headers      map[string][]byte
	}
	tests := []struct {
		name string
		args args
		want kafka.Message
	}{
		{
			name: "populated RequestContext",
			args: args{
				kafkaMessage: kafka.Message{},
				headers: map[string][]byte{
					"x-b3-traceid":        []byte("2"),
					"x-request-id":        []byte("1"),
					"x-user-id":           []byte("userID1"),
					"x-application-trail": []byte("trail"),
				},
			},
			want: kafka.Message{
				Headers: []kafka.Header{
					{
						Key:   "x-b3-traceid",
						Value: []byte("2"),
					},
					{
						Key:   "x-request-id",
						Value: []byte("1"),
					},
					{
						Key:   "x-user-id",
						Value: []byte("userID1"),
					},
					{
						Key:   "x-application-trail",
						Value: []byte("trail"),
					},
				},
			},
		},
		{
			name: "conflicting-fields-are-overwritten",
			args: args{
				kafkaMessage: kafka.Message{
					Headers: []kafka.Header{
						{
							Key:   "x-request-id",
							Value: []byte("999"),
						},
						{
							Key:   "extra-header",
							Value: []byte("77"),
						},
					},
				},
				headers: map[string][]byte{
					"x-b3-traceid":        []byte("2"),
					"x-request-id":        []byte("1"),
					"x-user-id":           []byte("userID1"),
					"x-application-trail": []byte("trail"),
				},
			},
			want: kafka.Message{
				Headers: []kafka.Header{
					{
						Key:   "x-b3-traceid",
						Value: []byte("2"),
					},
					{
						Key:   "x-request-id",
						Value: []byte("1"),
					},
					{
						Key:   "x-user-id",
						Value: []byte("userID1"),
					},
					{
						Key:   "x-application-trail",
						Value: []byte("trail"),
					},
					{
						Key:   "extra-header",
						Value: []byte("77"),
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer recoverThenFail(t)

			got := addHeaders(tt.args.kafkaMessage, tt.args.headers)

			require.ElementsMatch(t, tt.want.Headers, got.Headers, "RecordHeaders do not match")
		})
	}
}

func TestMessage_Value(t *testing.T) {
	defer recoverThenFail(t)

	valueStr := "here is some string"
	m := Message{
		value: []byte(valueStr),
	}
	got := m.Value()
	require.NotSame(t, m.value, got, "should not return the same reference")
	require.Equal(t, valueStr, string(got), "should return the same string representation")
}

func TestMessage_Value_HandleNil(t *testing.T) {
	defer recoverThenFail(t)

	m := Message{
		value: nil,
	}
	got := m.Value()
	require.Nil(t, got, "should have returned nil since underlying data is nil")
}
