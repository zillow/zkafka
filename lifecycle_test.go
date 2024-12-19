package zkafka

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_LifecycleChainedHooksAreCalled(t *testing.T) {
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
