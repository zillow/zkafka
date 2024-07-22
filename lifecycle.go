package zstreams

//go:generate mockgen -package=mock_zstreams -destination=./mocks/mock_lifecycle.go -source=./lifecycle.go

import (
	"context"
	"errors"
	"time"
)

type LifecyclePostReadMeta struct {
	Topic   string
	GroupID string
	// Message that was read
	Message *Message
}
type LifecyclePreProcessingMeta struct {
	Topic                 string
	GroupID               string
	VirtualPartitionIndex int
	// Time since the message was sent to the topic
	TopicLag time.Duration
	// Message containing being processed
	Message *Message
}

type LifecyclePostProcessingMeta struct {
	Topic                 string
	GroupID               string
	VirtualPartitionIndex int
	// Time taken to process the message
	ProcessingTime time.Duration
	// Message processed
	Msg *Message
	// Response code returned by the processor
	ResponseErr error
}

type LifecyclePostAckMeta struct {
	Topic string
	// Time when the message was published to the queue
	ProduceTime time.Time
}

type LifecyclePreWriteMeta struct{}

type LifecyclePreWriteResp struct {
	Headers map[string][]byte
}

type LifecycleHooks struct {
	// Called by work after reading a message, offers the ability to customize the context object (resulting context object passed to work processor)
	PostRead func(ctx context.Context, meta LifecyclePostReadMeta) (context.Context, error)

	// Called after receiving a message and before processing it.
	PreProcessing func(ctx context.Context, meta LifecyclePreProcessingMeta) (context.Context, error)

	// Called after processing a message
	PostProcessing func(ctx context.Context, meta LifecyclePostProcessingMeta) error

	// Called after sending a message to the queue
	PostAck func(ctx context.Context, meta LifecyclePostAckMeta) error

	// Called prior to executing write operation
	PreWrite func(ctx context.Context, meta LifecyclePreWriteMeta) (LifecyclePreWriteResp, error)

	// Call after the reader attempts a fanout call.
	PostFanout func(ctx context.Context)
}

// ChainLifecycleHooks chains multiple lifecycle hooks into one.  The hooks are
// called in the order they are passed.  All hooks are called, even when
// errors occur.  Errors are accumulated in a wrapper error and returned to the
// caller.
func ChainLifecycleHooks(hooks ...LifecycleHooks) LifecycleHooks {
	if len(hooks) == 0 {
		return LifecycleHooks{}
	}
	if len(hooks) == 1 {
		return hooks[0]
	}
	return LifecycleHooks{
		PreProcessing: func(ctx context.Context, meta LifecyclePreProcessingMeta) (context.Context, error) {
			var allErrs error

			hookCtx := ctx

			for _, h := range hooks {
				if h.PreProcessing != nil {
					var err error

					hookCtx, err = h.PreProcessing(hookCtx, meta)
					if err != nil {
						allErrs = errors.Join(allErrs, err)
					}
				}
			}

			return hookCtx, allErrs
		},

		PostProcessing: func(ctx context.Context, meta LifecyclePostProcessingMeta) error {
			var allErrs error

			for _, h := range hooks {
				if h.PostProcessing != nil {
					err := h.PostProcessing(ctx, meta)
					if err != nil {
						allErrs = errors.Join(allErrs, err)
					}
				}
			}

			return allErrs
		},

		PostAck: func(ctx context.Context, meta LifecyclePostAckMeta) error {
			var allErrs error

			for _, h := range hooks {
				if h.PostAck != nil {
					err := h.PostAck(ctx, meta)
					if err != nil {
						allErrs = errors.Join(allErrs, err)
					}
				}
			}

			return allErrs
		},
	}
}
