package mock_zstreams

//go:generate mockgen -source=kwriter.go -package mock_zstreams -mock_names writer=MockWriter -destination=./mock_writer.go

import (
	"context"

	"gitlab.zgtools.net/devex/archetypes/gomods/zstreams/v4"
)

var (
	_ writer          = (*zstreams.KWriter)(nil)
	_ zstreams.Writer = (writer)(nil)
)

type writer interface {
	Write(ctx context.Context, value any, opts ...zstreams.WriteOption) (zstreams.Response, error)
	WriteKey(ctx context.Context, key string, value any, opts ...zstreams.WriteOption) (zstreams.Response, error)
	WriteRaw(ctx context.Context, key *string, value []byte, opts ...zstreams.WriteOption) (zstreams.Response, error)
	Close()
}
