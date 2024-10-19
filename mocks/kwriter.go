package mock_zkafka

//go:generate mockgen -source=kwriter.go -package mock_zkafka -mock_names writer=MockWriter -destination=./mock_writer.go

import (
	"context"

	"github.com/zillow/zkafka/v2"
)

var (
	_ writer        = (*zkafka.KWriter)(nil)
	_ zkafka.Writer = (writer)(nil)
)

type writer interface {
	Write(ctx context.Context, value any, opts ...zkafka.WriteOption) (zkafka.Response, error)
	WriteKey(ctx context.Context, key string, value any, opts ...zkafka.WriteOption) (zkafka.Response, error)
	WriteRaw(ctx context.Context, key *string, value []byte, opts ...zkafka.WriteOption) (zkafka.Response, error)
	Close()
}
