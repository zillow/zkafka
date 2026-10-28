package zkafka

import (
	"fmt"
	"io"
)

// stringFormatter is a pass-through test marshaler. It passes strings, byte
// slices and io.Readers through as raw bytes; Unmarshal writes into an io.Writer.
type stringFormatter struct{}

func (stringFormatter) Marshal(i any) ([]byte, error) {
	switch v := i.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	case io.Reader:
		return io.ReadAll(v)
	default:
		return nil, fmt.Errorf("cannot convert to string %T", i)
	}
}

func (stringFormatter) Unmarshal(b []byte, i any) error {
	switch v := i.(type) {
	case io.Writer:
		_, err := v.Write(b)
		return err
	default:
		return fmt.Errorf("cannot convert from binary %T", i)
	}
}
