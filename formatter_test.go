package zkafka

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNoopFormatter_Marshall_Unmarshal(t *testing.T) {
	defer recoverThenFail(t)
	fmtter := errFormatter{}
	_, err := fmtter.Marshall(marshReq{subject: "anything"})
	require.ErrorIs(t, err, errMissingFmtter)

	var someInt int32
	err = fmtter.Unmarshal(unmarshReq{data: []byte("test"), target: &someInt})
	require.ErrorIs(t, err, errMissingFmtter)
}
