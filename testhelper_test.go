package zkafka

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_getFakeMessage(t *testing.T) {
	defer recoverThenFail(t)

	msg := getFakeMessage("key", "value", KMarshalerShim{F: JSONMarshaler{}}, nil)
	expectedMessage := Message{
		Key:   "key",
		value: []byte("\"value\""),
	}
	require.Equal(t, expectedMessage.Key, msg.Key, "Expected generated zkafka.Message to use key from arg")
	require.Equal(t, string(expectedMessage.value), string(msg.value), "Expected generated zkafka.Message to use value from arg")
}

func TestGetFakeMessageFromFake(t *testing.T) {
	defer recoverThenFail(t)

	fmtr := JSONMarshaler{}
	val, err := fmtr.Marshal("value")
	require.NoError(t, err)
	msg := GetMsgFromFake(&FakeMessage{
		Key:       new("key"),
		Value:     val,
		Marshaler: KMarshalerShim{F: fmtr},
	})
	expectedMessage := Message{
		Key:   "key",
		value: []byte("\"value\""),
	}
	require.Equal(t, expectedMessage.Key, msg.Key, "Expected generated zkafka.Message to use key from arg")
	require.Equal(t, string(expectedMessage.value), string(msg.value), "Expected generated zkafka.Message to use value from arg")
}

func TestMsgFromFake_WhenMarshalError(t *testing.T) {
	// pass in some invalid object for marshaling
	msg := GetMsgFromFake(&FakeMessage{
		Key:       new("key"),
		ValueData: make(chan int),
		Marshaler: KMarshalerShim{F: JSONMarshaler{}},
	})
	expectedMessage := Message{
		Key:   "key",
		value: nil,
	}
	require.Equal(t, expectedMessage.Key, msg.Key, "Expected generated zkafka.Message to use key from arg")
	require.Equal(t, string(expectedMessage.value), string(msg.value), "Expected generated zkafka.Message to use value from arg")
}

func TestGetFakeMessage_UsesMarshalerForValue(t *testing.T) {
	// arrange
	called := false

	// act
	msg := GetFakeMessage("key", map[string]string{"hello": "world"}, KMarshalerShim{F: JSONMarshaler{}}, func() { called = true })
	var got map[string]string
	err := msg.Decode(&got)
	msg.Done()

	// assert
	require.NoError(t, err)
	require.Equal(t, "key", msg.Key)
	require.Equal(t, map[string]string{"hello": "world"}, got)
	require.True(t, called)
}
