package zkafka

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
)

func Test_msgCarrier_Get(t *testing.T) {
	tests := []struct {
		name string
		msg  *Message
		key  string
		want string
	}{
		{
			name: "",
			msg:  &Message{Headers: map[string][]byte{"key": []byte("value")}},
			key:  "key",
			want: "value",
		},
		{
			name: "",
			msg:  &Message{Headers: map[string][]byte{"key": {4, 9, 1, 32, 99}}},
			key:  "key",
			want: "\u0004\t\u0001 c",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &msgCarrier{
				msg: tt.msg,
			}
			got := c.Get(tt.key)
			require.Equal(t, got, tt.want)
		})
	}
}

func Test_msgCarrier_Keys(t *testing.T) {
	tests := []struct {
		name string
		msg  *Message
		want []string
	}{
		{
			name: "",
			msg:  &Message{Headers: map[string][]byte{"key": []byte("value"), "key2": []byte("value2")}},
			want: []string{"key", "key2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &msgCarrier{
				msg: tt.msg,
			}
			got := c.Keys()
			require.ElementsMatch(t, got, tt.want)
		})
	}
}

func Test_msgCarrier_Set_IsANoop(t *testing.T) {
	msg := &Message{Headers: map[string][]byte{"key": []byte("value"), "key2": []byte("value2")}}
	c := &msgCarrier{
		msg: msg,
	}
	c.Set("key", "dog")
	require.Equal(t, c.Get("key"), "value")
}

func Test_kMsgCarrier_Set(t *testing.T) {
	msg := &kafka.Message{Headers: []kafka.Header{
		{
			Key:   "key",
			Value: []byte("value"),
		},
		{
			Key:   "key2",
			Value: []byte("value2"),
		},
	}}
	c := &kMsgCarrier{
		msg: msg,
	}
	c.Set("key", "dog")
	c.Set("hello", "world")
	require.Equal(t, "dog", c.Get("key"))
	require.Equal(t, "world", c.Get("hello"))
	require.Equal(t, "value2", c.Get("key2"))
	require.Len(t, c.Keys(), 3)
}
