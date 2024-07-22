package mock_zkafka

//go:generate mockgen -source=kreader.go -package mock_zkafka -mock_names reader=MockReader -destination=./mock_reader.go

import (
	"context"

	"github.com/zillow/zkafka"
)

var (
	_ reader        = (*zkafka.KReader)(nil)
	_ zkafka.Reader = (reader)(nil)
)

type reader interface {
	Read(ctx context.Context) (*zkafka.Message, error)
	Close() error
	Assignments(_ context.Context) ([]zkafka.Assignment, error)
}
