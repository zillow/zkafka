package zkafka

//go:generate mockgen -destination=./mocks/mock_logger.go -source=logger.go

import (
	"context"
)

// Logger is the interface that wraps basic logging functions
type Logger interface {
	Debugw(ctx context.Context, msg string, keysAndValues ...any)
	Infow(ctx context.Context, msg string, keysAndValues ...any)
	Warnw(ctx context.Context, msg string, keysAndValues ...any)
	Errorw(ctx context.Context, msg string, keysAndValues ...any)
}

var _ Logger = (*NoopLogger)(nil)

type NoopLogger struct{}

func (l NoopLogger) Debugw(_ context.Context, _ string, _ ...any) {}

func (l NoopLogger) Infow(_ context.Context, _ string, _ ...any) {}

func (l NoopLogger) Warnw(_ context.Context, _ string, _ ...any)  {}
func (l NoopLogger) Errorw(_ context.Context, _ string, _ ...any) {}
