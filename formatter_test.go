package zstreams

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNoopFormatter_Marshall_Unmarshal(t *testing.T) {
	defer recoverThenFail(t)
	fmtter := noopFormatter{}
	_, err := fmtter.Marshall("anything")
	require.ErrorIs(t, err, errMissingFmtter)

	var someInt int32
	err = fmtter.Unmarshal([]byte("test"), &someInt)
	require.ErrorIs(t, err, errMissingFmtter)
}
