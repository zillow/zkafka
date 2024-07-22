package zstreams

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_LifecycleChainedHooksAreCalled(t *testing.T) {
	lhState := make(map[string]int) // Map from state to number of times called

	hooks1 := LifecycleHooks{
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
	}

	hooks2 := LifecycleHooks{
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
	}

	lh := ChainLifecycleHooks(hooks1, hooks2)

	lh.PreProcessing(context.Background(), LifecyclePreProcessingMeta{})
	require.Equal(t, 1, lhState["hooks1-pre-processing"], "hooks1-pre-processing not called")
	require.Equal(t, 1, lhState["hooks2-pre-processing"], "hooks2-pre-processing not called")
	require.Equal(t, 0, lhState["hooks1-post-processing"], "hooks1-post-processing called")
	require.Equal(t, 0, lhState["hooks2-post-processing"], "hooks2-post-processing called")
	require.Equal(t, 0, lhState["hooks1-post-ack"], "hooks1-post-ack called")
	require.Equal(t, 0, lhState["hooks2-post-ack"], "hooks2-post-ack called")

	lh.PostProcessing(context.Background(), LifecyclePostProcessingMeta{})
	require.Equal(t, 1, lhState["hooks1-pre-processing"], "hooks1-pre-processing not called")
	require.Equal(t, 1, lhState["hooks2-pre-processing"], "hooks2-pre-processing not called")
	require.Equal(t, 1, lhState["hooks1-post-processing"], "hooks1-post-processing not called")
	require.Equal(t, 1, lhState["hooks2-post-processing"], "hooks2-post-processing not called")
	require.Equal(t, 0, lhState["hooks1-post-ack"], "hooks1-post-ack called")
	require.Equal(t, 0, lhState["hooks2-post-ack"], "hooks2-post-ack called")

	lh.PostAck(context.Background(), LifecyclePostAckMeta{})
	require.Equal(t, 1, lhState["hooks1-pre-processing"], "hooks1-pre-processing not called")
	require.Equal(t, 1, lhState["hooks2-pre-processing"], "hooks2-pre-processing not called")
	require.Equal(t, 1, lhState["hooks1-post-processing"], "hooks1-post-processing not called")
	require.Equal(t, 1, lhState["hooks2-post-processing"], "hooks2-post-processing not called")
	require.Equal(t, 1, lhState["hooks1-post-ack"], "hooks1-post-ack not called")
	require.Equal(t, 1, lhState["hooks2-post-ack"], "hooks2-post-ack not called")
}
