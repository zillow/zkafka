package mock_zstreams

//go:generate mockgen -source=kreader.go -package mock_zstreams -mock_names reader=MockReader -destination=./mock_reader.go

import (
	"context"

	"gitlab.zgtools.net/devex/archetypes/gomods/zstreams/v4"
)

var (
	_ reader          = (*zstreams.KReader)(nil)
	_ zstreams.Reader = (reader)(nil)
)

type reader interface {
	Read(ctx context.Context) (*zstreams.Message, error)
	Close() error
	Assignments(_ context.Context) ([]zstreams.Assignment, error)
}
