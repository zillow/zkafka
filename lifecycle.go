package zkafka

import (
	"context"
	"errors"
	"time"
)

type LifecyclePostReadMeta struct {
	Topic   string
	GroupID string
	// Message that was read (will be non nil)
	Message *Message
}

type LifecyclePostReadImmediateMeta struct {
	// Message that was read (could be nil)
	Message *Message
	Err     error
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
	// Called by work after reading a message (guaranteed non nil), offers the ability to customize the context object (resulting context object passed to work processor)
	PostRead func(ctx context.Context, meta LifecyclePostReadMeta) (context.Context, error)

	// Called by work immediately after an attempt to read a message. Msg might be nil, if there was an error
	// or no available messages.
	PostReadImmediate func(ctx context.Context, meta LifecyclePostReadImmediateMeta)

	// Called after receiving a message and before processing it.
	PreProcessing func(ctx context.Context, meta LifecyclePreProcessingMeta) (context.Context, error)

	// Called after processing a message
	PostProcessing func(ctx context.Context, meta LifecyclePostProcessingMeta) error

	// Called after sending a message to the queue
	PostAck func(ctx context.Context, meta LifecyclePostAckMeta) error

	// Called prior to executing write operation
	PreWrite func(ctx context.Context, meta LifecyclePreWriteMeta) (LifecyclePreWriteResp, error)

	// Call after the reader attempts a fanOut call.
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
		PostRead: func(ctx context.Context, meta LifecyclePostReadMeta) (context.Context, error) {
			var allErrs error

			hookCtx := ctx

			for _, h := range hooks {
				if h.PostRead != nil {
					var err error

					hookCtx, err = h.PostRead(hookCtx, meta)
					if err != nil {
						allErrs = errors.Join(allErrs, err)
					}
				}
			}

			return hookCtx, allErrs

		},
		PostReadImmediate: func(ctx context.Context, meta LifecyclePostReadImmediateMeta) {
			for _, h := range hooks {
				if h.PostReadImmediate != nil {
					h.PostReadImmediate(ctx, meta)
				}
			}
		},
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
		PreWrite: func(ctx context.Context, meta LifecyclePreWriteMeta) (LifecyclePreWriteResp, error) {
			var allErrs error

			out := LifecyclePreWriteResp{
				Headers: make(map[string][]byte),
			}
			for _, h := range hooks {
				if h.PreProcessing != nil {
					var err error

					resp, err := h.PreWrite(ctx, meta)
					if err != nil {
						allErrs = errors.Join(allErrs, err)
					}
					for k, v := range resp.Headers {
						out.Headers[k] = v
					}
				}
			}

			return out, allErrs
		},
		PostFanout: func(ctx context.Context) {
			for _, h := range hooks {
				if h.PostRead != nil {
					h.PostFanout(ctx)
				}
			}
		},
	}
}
