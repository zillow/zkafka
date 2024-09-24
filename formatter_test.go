package zkafka

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNoopFormatter_Marshall_Unmarshal(t *testing.T) {
	defer recoverThenFail(t)
	formatter := errFormatter{}
	_, err := formatter.marshall(marshReq{v: "anything"})
	require.ErrorIs(t, err, errMissingFormatter)

	var someInt int32
	err = formatter.unmarshal(unmarshReq{data: []byte("test"), target: &someInt})
	require.ErrorIs(t, err, errMissingFormatter)
}
