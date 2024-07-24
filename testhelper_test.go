package zkafka

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zillow/zfmt"
)

func Test_getFakeMessage(t *testing.T) {
	defer recoverThenFail(t)

	msg := getFakeMessage("key", "value", &zfmt.JSONFormatter{}, nil)
	expectedMessage := Message{
		Key:   "key",
		value: []byte("\"value\""),
	}
	require.Equal(t, expectedMessage.Key, msg.Key, "Expected generated zkafka.Message to use key from arg")
	require.Equal(t, string(expectedMessage.value), string(msg.value), "Expected generated zkafka.Message to use value from arg")
}

func TestGetFakeMessageFromFake(t *testing.T) {
	defer recoverThenFail(t)

	fmtr := &zfmt.JSONFormatter{}
	val, err := fmtr.Marshall("value")
	require.NoError(t, err)
	msg := GetMsgFromFake(&FakeMessage{
		Key:   ptr("key"),
		Value: val,
		Fmt:   fmtr,
	})
	expectedMessage := Message{
		Key:   "key",
		value: []byte("\"value\""),
	}
	require.Equal(t, expectedMessage.Key, msg.Key, "Expected generated zkafka.Message to use key from arg")
	require.Equal(t, string(expectedMessage.value), string(msg.value), "Expected generated zkafka.Message to use value from arg")
}

func TestMsgFromFake_WhenMarshallError(t *testing.T) {
	// pass in some invalid object for marshalling
	msg := GetMsgFromFake(&FakeMessage{
		Key:       ptr("key"),
		ValueData: make(chan int),
		Fmt:       &zfmt.JSONFormatter{},
	})
	expectedMessage := Message{
		Key:   "key",
		value: nil,
	}
	require.Equal(t, expectedMessage.Key, msg.Key, "Expected generated zkafka.Message to use key from arg")
	require.Equal(t, string(expectedMessage.value), string(msg.value), "Expected generated zkafka.Message to use value from arg")
}
