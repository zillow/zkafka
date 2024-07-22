package zkafka

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.zgtools.net/devex/archetypes/gomods/zfmt"
)

func TestGetFakeMessage(t *testing.T) {
	defer recoverThenFail(t)

	msg := GetFakeMessage("key", "value", &zfmt.JSONFormatter{}, nil)
	expectedMessage := Message{
		Key:   "key",
		value: []byte("\"value\""),
	}
	require.Equal(t, expectedMessage.Key, msg.Key, "Expected generated zkafka.Message to use key from arg")
	require.Equal(t, string(expectedMessage.value), string(msg.value), "Expected generated zkafka.Message to use value from arg")
}

func TestGetFakeMessageFromRaw(t *testing.T) {
	defer recoverThenFail(t)

	fmtr := &zfmt.JSONFormatter{}
	val, err := fmtr.Marshall("value")
	require.NoError(t, err)
	msg := GetFakeMsgFromRaw(ptr("key"), val, fmtr, nil)
	expectedMessage := Message{
		Key:   "key",
		value: []byte("\"value\""),
	}
	require.Equal(t, expectedMessage.Key, msg.Key, "Expected generated zkafka.Message to use key from arg")
	require.Equal(t, string(expectedMessage.value), string(msg.value), "Expected generated zkafka.Message to use value from arg")
}

func TestGetFakeMessage_WhenMarshallError(t *testing.T) {

	// pass in some invalid object for marshalling
	msg := GetFakeMessage("key", make(chan int), &zfmt.JSONFormatter{}, nil)
	expectedMessage := Message{
		Key:   "key",
		value: nil,
	}
	require.Equal(t, expectedMessage.Key, msg.Key, "Expected generated zkafka.Message to use key from arg")
	require.Equal(t, string(expectedMessage.value), string(msg.value), "Expected generated zkafka.Message to use value from arg")
}
