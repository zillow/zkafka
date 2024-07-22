//go:build integration
// +build integration

package test

import (
	"context"
	"sync"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"gitlab.zgtools.net/devex/archetypes/gomods/zfmt"
	"gitlab.zgtools.net/devex/archetypes/gomods/zstreams/v4"
	mock_confluent "gitlab.zgtools.net/devex/archetypes/gomods/zstreams/v4/mocks/confluent"
)

func TestWriter_Write_LifecycleHooksCalled(t *testing.T) {
	ctx, f := context.WithCancel(context.Background())
	defer f()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lhMtx := sync.Mutex{}
	lhState := FakeLifecycleState{
		numCalls: map[string]int{},
	}
	lh := NewFakeLifecycleHooks(&lhMtx, &lhState)

	bootstrapServer := getBootstrap()

	mockProducer := mock_confluent.NewMockKafkaProducer(ctrl)
	mockProducer.EXPECT().Close().AnyTimes()
	mockProducer.EXPECT().Produce(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(msg *kafka.Message, deliveryChan chan kafka.Event) error {
		go func() {
			deliveryChan <- &kafka.Message{}
		}()
		return nil
	})

	client := zstreams.NewClient(zstreams.Config{BootstrapServers: []string{bootstrapServer}},
		zstreams.LoggerOption(stdLogger{}),
		zstreams.WithClientLifecycleHooks(lh),
		zstreams.WithProducerProvider(func(config map[string]any) (zstreams.KafkaProducer, error) {
			return mockProducer, nil
		}),
	)
	defer func() { _ = client.Close() }()

	writer, err := client.Writer(ctx, zstreams.ProducerTopicConfig{
		ClientID:  "writer",
		Topic:     "topic",
		Formatter: zfmt.JSONFmt,
	})
	require.NoError(t, err)

	msg := Msg{Val: "1"}
	_, err = writer.Write(ctx, msg)
	require.NoError(t, err)

	require.Equal(t, 0, lhState.numCalls["pre-processing"])
	require.Equal(t, 0, lhState.numCalls["post-processing"])
	require.Equal(t, 1, lhState.numCalls["post-ack"])
	require.Equal(t, 1, lhState.numCalls["pre-write"])
}
