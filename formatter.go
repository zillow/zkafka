package zkafka

import (
	"errors"

	"gitlab.zgtools.net/devex/archetypes/gomods/zfmt"
)

const (
	// CustomFmt indicates that the user would pass in their own Formatter later
	CustomFmt zfmt.FormatterType = "custom"
)

var errMissingFmtter = errors.New("custom formatter is missing, did you forget to call WithFormatter()")

// Formatter allows the user to extend formatting capability to unsupported data types
type Formatter interface {
	Marshall(v any) ([]byte, error)
	Unmarshal(b []byte, v any) error
}

// noopFormatter is a formatter that returns error when called. The error will remind the user
// to provide appropriate implementation
type noopFormatter struct{}

// Marshall returns error with reminder
func (f noopFormatter) Marshall(_ any) ([]byte, error) {
	return nil, errMissingFmtter
}

// Unmarshal returns error with reminder
func (f noopFormatter) Unmarshal(_ []byte, _ any) error {
	return errMissingFmtter
}
