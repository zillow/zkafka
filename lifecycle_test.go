package zkafka

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_LifecycleChainedHooksAreCalled(t *testing.T) {
	defer recoverThenFail(t)
	lhState := make(map[string]int) // Map from state to number of times called

	hooks1 := LifecycleHooks{
		PostRead: func(ctx context.Context, meta LifecyclePostReadMeta) (context.Context, error) {
			lhState["hooks1-post-read"] += 1
			return ctx, nil
		},
		PostReadImmediate: func(ctx context.Context, meta LifecyclePostReadImmediateMeta) {
			lhState["hooks1-post-read-immediate"] += 1
		},
		PreProcessing: func(ctx context.Context, meta LifecyclePreProcessingMeta) (context.Context, error) {
			lhState["hooks1-pre-processing"] += 1
			return ctx, nil
		},
		PostProcessing: func(ctx context.Context, meta LifecyclePostProcessingMeta) error {
			lhState["hooks1-post-processing"] += 1
			return nil
		},
		PostAck: func(ctx context.Context, meta LifecyclePostAckMeta) error {
			lhState["hooks1-post-ack"] += 1
			return nil
		},
		PreWrite: func(ctx context.Context, meta LifecyclePreWriteMeta) (LifecyclePreWriteResp, error) {
			lhState["hooks1-pre-write"] += 1
			return LifecyclePreWriteResp{}, nil
		},
		PostFanout: func(ctx context.Context) {
			lhState["hooks1-post-fanout"] += 1
		},
	}

	hooks2 := LifecycleHooks{
		PostRead: func(ctx context.Context, meta LifecyclePostReadMeta) (context.Context, error) {
			lhState["hooks2-post-read"] += 1
			return ctx, nil
		},
		PostReadImmediate: func(ctx context.Context, meta LifecyclePostReadImmediateMeta) {
			lhState["hooks2-post-read-immediate"] += 1
		},
		PreProcessing: func(ctx context.Context, meta LifecyclePreProcessingMeta) (context.Context, error) {
			lhState["hooks2-pre-processing"] += 1
			return ctx, nil
		},
		PostProcessing: func(ctx context.Context, meta LifecyclePostProcessingMeta) error {
			lhState["hooks2-post-processing"] += 1
			return nil
		},
		PostAck: func(ctx context.Context, meta LifecyclePostAckMeta) error {
			lhState["hooks2-post-ack"] += 1
			return nil
		},
		PreWrite: func(ctx context.Context, meta LifecyclePreWriteMeta) (LifecyclePreWriteResp, error) {
			lhState["hooks2-pre-write"] += 1
			return LifecyclePreWriteResp{}, nil
		},
		PostFanout: func(ctx context.Context) {
			lhState["hooks2-post-fanout"] += 1
		},
	}

	lh := ChainLifecycleHooks(hooks1, hooks2)

	_, err := lh.PreProcessing(context.Background(), LifecyclePreProcessingMeta{})
	require.NoError(t, err)
	require.Equal(t, 1, lhState["hooks1-pre-processing"])
	require.Equal(t, 1, lhState["hooks2-pre-processing"])
	require.Equal(t, 0, lhState["hooks1-post-processing"])
	require.Equal(t, 0, lhState["hooks2-post-processing"])
	require.Equal(t, 0, lhState["hooks1-post-ack"])
	require.Equal(t, 0, lhState["hooks2-post-ack"])
	require.Equal(t, 0, lhState["hooks1-post-read"])
	require.Equal(t, 0, lhState["hooks2-post-read"])
	require.Equal(t, 0, lhState["hooks1-post-ack"])
	require.Equal(t, 0, lhState["hooks2-post-ack"])
	require.Equal(t, 0, lhState["hooks1-pre-write"])
	require.Equal(t, 0, lhState["hooks2-pre-write"])
	require.Equal(t, 0, lhState["hooks1-post-fanout"])
	require.Equal(t, 0, lhState["hooks2-post-fanout"])
	require.Equal(t, 0, lhState["hooks1-post-read-immediate"])
	require.Equal(t, 0, lhState["hooks2-post-read-immediate"])

	err = lh.PostProcessing(context.Background(), LifecyclePostProcessingMeta{})
	require.NoError(t, err)
	require.Equal(t, 1, lhState["hooks1-pre-processing"])
	require.Equal(t, 1, lhState["hooks2-pre-processing"])
	require.Equal(t, 1, lhState["hooks1-post-processing"])
	require.Equal(t, 1, lhState["hooks2-post-processing"])
	require.Equal(t, 0, lhState["hooks1-post-ack"])
	require.Equal(t, 0, lhState["hooks2-post-ack"])
	require.Equal(t, 0, lhState["hooks1-post-read"])
	require.Equal(t, 0, lhState["hooks2-post-read"])
	require.Equal(t, 0, lhState["hooks1-post-ack"])
	require.Equal(t, 0, lhState["hooks2-post-ack"])
	require.Equal(t, 0, lhState["hooks1-pre-write"])
	require.Equal(t, 0, lhState["hooks2-pre-write"])
	require.Equal(t, 0, lhState["hooks1-post-fanout"])
	require.Equal(t, 0, lhState["hooks2-post-fanout"])
	require.Equal(t, 0, lhState["hooks1-post-read-immediate"])
	require.Equal(t, 0, lhState["hooks2-post-read-immediate"])

	_, err = lh.PostRead(context.Background(), LifecyclePostReadMeta{})
	require.NoError(t, err)
	require.Equal(t, 1, lhState["hooks1-pre-processing"])
	require.Equal(t, 1, lhState["hooks2-pre-processing"])
	require.Equal(t, 1, lhState["hooks1-post-processing"])
	require.Equal(t, 1, lhState["hooks2-post-processing"])
	require.Equal(t, 1, lhState["hooks1-post-read"])
	require.Equal(t, 1, lhState["hooks2-post-read"])
	require.Equal(t, 0, lhState["hooks1-post-ack"])
	require.Equal(t, 0, lhState["hooks2-post-ack"])
	require.Equal(t, 0, lhState["hooks1-post-ack"])
	require.Equal(t, 0, lhState["hooks2-post-ack"])
	require.Equal(t, 0, lhState["hooks1-pre-write"])
	require.Equal(t, 0, lhState["hooks2-pre-write"])
	require.Equal(t, 0, lhState["hooks1-post-fanout"])
	require.Equal(t, 0, lhState["hooks2-post-fanout"])
	require.Equal(t, 0, lhState["hooks1-post-read-immediate"])
	require.Equal(t, 0, lhState["hooks2-post-read-immediate"])

	err = lh.PostAck(context.Background(), LifecyclePostAckMeta{})
	require.NoError(t, err)
	require.Equal(t, 1, lhState["hooks1-pre-processing"])
	require.Equal(t, 1, lhState["hooks2-pre-processing"])
	require.Equal(t, 1, lhState["hooks1-post-processing"])
	require.Equal(t, 1, lhState["hooks2-post-processing"])
	require.Equal(t, 1, lhState["hooks1-post-read"])
	require.Equal(t, 1, lhState["hooks2-post-read"])
	require.Equal(t, 1, lhState["hooks1-post-ack"])
	require.Equal(t, 1, lhState["hooks2-post-ack"])
	require.Equal(t, 0, lhState["hooks1-pre-write"])
	require.Equal(t, 0, lhState["hooks2-pre-write"])
	require.Equal(t, 0, lhState["hooks1-post-fanout"])
	require.Equal(t, 0, lhState["hooks2-post-fanout"])
	require.Equal(t, 0, lhState["hooks1-post-read-immediate"])
	require.Equal(t, 0, lhState["hooks2-post-read-immediate"])

	_, err = lh.PreWrite(context.Background(), LifecyclePreWriteMeta{})
	require.NoError(t, err)
	require.Equal(t, 1, lhState["hooks1-pre-processing"])
	require.Equal(t, 1, lhState["hooks2-pre-processing"])
	require.Equal(t, 1, lhState["hooks1-post-processing"])
	require.Equal(t, 1, lhState["hooks2-post-processing"])
	require.Equal(t, 1, lhState["hooks1-post-read"])
	require.Equal(t, 1, lhState["hooks2-post-read"])
	require.Equal(t, 1, lhState["hooks1-post-ack"])
	require.Equal(t, 1, lhState["hooks2-post-ack"])
	require.Equal(t, 1, lhState["hooks1-pre-write"])
	require.Equal(t, 1, lhState["hooks2-pre-write"])
	require.Equal(t, 0, lhState["hooks1-post-fanout"])
	require.Equal(t, 0, lhState["hooks2-post-fanout"])
	require.Equal(t, 0, lhState["hooks1-post-read-immediate"])
	require.Equal(t, 0, lhState["hooks2-post-read-immediate"])

	lh.PostFanout(context.Background())
	require.Equal(t, 1, lhState["hooks1-pre-processing"])
	require.Equal(t, 1, lhState["hooks2-pre-processing"])
	require.Equal(t, 1, lhState["hooks1-post-processing"])
	require.Equal(t, 1, lhState["hooks2-post-processing"])
	require.Equal(t, 1, lhState["hooks1-post-read"])
	require.Equal(t, 1, lhState["hooks2-post-read"])
	require.Equal(t, 1, lhState["hooks1-post-ack"])
	require.Equal(t, 1, lhState["hooks2-post-ack"])
	require.Equal(t, 1, lhState["hooks1-pre-write"])
	require.Equal(t, 1, lhState["hooks2-pre-write"])
	require.Equal(t, 1, lhState["hooks1-post-fanout"])
	require.Equal(t, 1, lhState["hooks2-post-fanout"])
	require.Equal(t, 0, lhState["hooks1-post-read-immediate"])
	require.Equal(t, 0, lhState["hooks2-post-read-immediate"])

	lh.PostReadImmediate(context.Background(), LifecyclePostReadImmediateMeta{})
	require.Equal(t, 1, lhState["hooks1-pre-processing"])
	require.Equal(t, 1, lhState["hooks2-pre-processing"])
	require.Equal(t, 1, lhState["hooks1-post-processing"])
	require.Equal(t, 1, lhState["hooks2-post-processing"])
	require.Equal(t, 1, lhState["hooks1-post-ack"])
	require.Equal(t, 1, lhState["hooks2-post-ack"])
	require.Equal(t, 1, lhState["hooks1-post-read"])
	require.Equal(t, 1, lhState["hooks2-post-read"])
	require.Equal(t, 1, lhState["hooks1-post-ack"])
	require.Equal(t, 1, lhState["hooks2-post-ack"])
	require.Equal(t, 1, lhState["hooks1-pre-write"])
	require.Equal(t, 1, lhState["hooks2-pre-write"])
	require.Equal(t, 1, lhState["hooks1-post-fanout"])
	require.Equal(t, 1, lhState["hooks2-post-fanout"])
	require.Equal(t, 1, lhState["hooks1-post-read-immediate"])
	require.Equal(t, 1, lhState["hooks2-post-read-immediate"])
}

// Test_LifecycleChainedNilPostReadImmediateInvocation confirms the invocation of a lifecycle hook method constructed
// via `ChainLifecycleHooks` which is nil doesn't panic. This in response to a bug where incorrect nil checks resulted
// in panicked invocations
func Test_LifecycleChainedNilPostReadImmediateInvocation(t *testing.T) {
	defer recoverThenFail(t)

	h1 := noopLifecycleHooks()
	h1.PostReadImmediate = nil
	h2 := LifecycleHooks{}
	chained := ChainLifecycleHooks(h1, h2)

	chained.PostReadImmediate(context.Background(), LifecyclePostReadImmediateMeta{})
}

// Test_LifecycleChainedNilPostReadInvocation confirms the invocation of a lifecycle hook method constructed
// via `ChainLifecycleHooks` which is nil doesn't panic. This in response to a bug where incorrect nil checks resulted
// in panicked invocations
func Test_LifecycleChainedNilPostReadInvocation(t *testing.T) {
	defer recoverThenFail(t)

	h1 := noopLifecycleHooks()
	h1.PostRead = nil
	h2 := LifecycleHooks{}
	chained := ChainLifecycleHooks(h1, h2)

	_, err := chained.PostRead(context.Background(), LifecyclePostReadMeta{})
	require.NoError(t, err)
}

// Test_LifecycleChainedNilPreProcessingInvocation confirms the invocation of a lifecycle hook method constructed
// via `ChainLifecycleHooks` which is nil doesn't panic. This in response to a bug where incorrect nil checks resulted
// in panicked invocations
func Test_LifecycleChainedNilPreProcessingInvocation(t *testing.T) {
	defer recoverThenFail(t)

	h1 := noopLifecycleHooks()
	h1.PreProcessing = nil
	h2 := LifecycleHooks{}
	chained := ChainLifecycleHooks(h1, h2)

	_, err := chained.PreProcessing(context.Background(), LifecyclePreProcessingMeta{})
	require.NoError(t, err)
}

// Test_LifecycleChainedNilPostProcessingInvocation confirms the invocation of a lifecycle hook method constructed
// via `ChainLifecycleHooks` which is nil doesn't panic. This in response to a bug where incorrect nil checks resulted
// in panicked invocations
func Test_LifecycleChainedNilPostProcessingInvocation(t *testing.T) {
	defer recoverThenFail(t)

	h1 := noopLifecycleHooks()
	h1.PostProcessing = nil
	h2 := LifecycleHooks{}
	chained := ChainLifecycleHooks(h1, h2)

	err := chained.PostProcessing(context.Background(), LifecyclePostProcessingMeta{})
	require.NoError(t, err)
}

// Test_LifecycleChainedNiPostAckInvocation confirms the invocation of a lifecycle hook method constructed
// via `ChainLifecycleHooks` which is nil doesn't panic. This in response to a bug where incorrect nil checks resulted
// in panicked invocations
func Test_LifecycleChainedNiPostAckInvocation(t *testing.T) {
	defer recoverThenFail(t)

	h1 := noopLifecycleHooks()
	h1.PostAck = nil
	h2 := LifecycleHooks{}
	chained := ChainLifecycleHooks(h1, h2)

	err := chained.PostAck(context.Background(), LifecyclePostAckMeta{})
	require.NoError(t, err)
}

// Test_LifecycleChainedNilPreWriteInvocation confirms the invocation of a lifecycle hook method constructed
// via `ChainLifecycleHooks` which is nil doesn't panic. This in response to a bug where incorrect nil checks resulted
// in panicked invocations
func Test_LifecycleChainedNilPreWriteInvocation(t *testing.T) {
	defer recoverThenFail(t)

	h1 := noopLifecycleHooks()
	h1.PreWrite = nil
	h2 := LifecycleHooks{}
	chained := ChainLifecycleHooks(h1, h2)

	_, err := chained.PreWrite(context.Background(), LifecyclePreWriteMeta{})
	require.NoError(t, err)
}

// Test_LifecycleChainedNilPostFanoutInvocation confirms the invocation of a lifecycle hook method constructed
// via `ChainLifecycleHooks` which is nil doesn't panic. This in response to a bug where incorrect nil checks resulted
// in panicked invocations
func Test_LifecycleChainedNilPostFanoutInvocation(t *testing.T) {
	defer recoverThenFail(t)

	h1 := noopLifecycleHooks()
	h1.PostFanout = nil
	h2 := LifecycleHooks{}
	chained := ChainLifecycleHooks(h1, h2)

	chained.PostFanout(context.Background())
}

func noopLifecycleHooks() LifecycleHooks {
	return LifecycleHooks{
		PostRead:          func(ctx context.Context, meta LifecyclePostReadMeta) (context.Context, error) { return nil, nil },
		PostReadImmediate: func(ctx context.Context, meta LifecyclePostReadImmediateMeta) {},
		PreProcessing:     func(ctx context.Context, meta LifecyclePreProcessingMeta) (context.Context, error) { return nil, nil },
		PostProcessing:    func(ctx context.Context, meta LifecyclePostProcessingMeta) error { return nil },
		PostAck:           func(ctx context.Context, meta LifecyclePostAckMeta) error { return nil },
		PreWrite: func(ctx context.Context, meta LifecyclePreWriteMeta) (LifecyclePreWriteResp, error) {
			return LifecyclePreWriteResp{}, nil
		},
		PostFanout: func(ctx context.Context) {},
	}
}
