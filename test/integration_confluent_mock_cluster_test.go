package test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/zillow/zfmt"
	"github.com/zillow/zkafka/v2"
)

// Exercises the circuit breaker lifecycle hooks against an in-process Kafka
// mock cluster
// (https://github.com/confluentinc/confluent-kafka-go/blob/master/examples/mockcluster_example/mockcluster.go).
//
// The topic is seeded with messages and a worker is started whose processor
// initially returns errors so the breaker trips into the open state. Once the
// breaker opens we flip the processor to succeed, allowing the next half-open
// probe to close the breaker. CircuitBreakerStateChanged should fire with
// To=open and then To=closed.
func Test_MockCluster_CircuitBreakerLifecycleHooksInvoked(t *testing.T) {
	defer recoverThenFail(t)

	mc, err := kafka.NewMockCluster(1)
	require.NoError(t, err)
	t.Cleanup(mc.Close)

	bootstrap := mc.BootstrapServers()

	topic := "cb-lifecycle-" + uuid.NewString()
	require.NoError(t, mc.CreateTopic(topic, 1, 1))

	clientID := fmt.Sprintf("%s-%s", t.Name(), uuid.NewString())
	groupID := uuid.NewString()

	l := stdLogger{}
	client := zkafka.NewClient(
		zkafka.Config{BootstrapServers: []string{bootstrap}},
		zkafka.LoggerOption(l),
	)
	t.Cleanup(func() { _ = client.Close() })

	// Seed the topic with enough messages to drive consecutive failures and at
	// least one success once the breaker re-opens to half-open.
	writer, err := client.Writer(t.Context(), zkafka.ProducerTopicConfig{
		ClientID:  clientID + "-writer",
		Topic:     topic,
		Formatter: zfmt.JSONFmt,
	})
	require.NoError(t, err)
	for i := range 20 {
		_, err := writer.Write(t.Context(), Msg{Val: fmt.Sprintf("seed-%d", i)})
		require.NoError(t, err)
	}

	failing := atomic.Bool{}
	failing.Store(true)

	processor := &fakeProcessor{
		process: func(ctx context.Context, msg *zkafka.Message) error {
			if failing.Load() {
				return errors.New("induced failure to trip circuit breaker")
			}
			return nil
		},
	}

	openedCount := atomic.Int64{}
	closedCount := atomic.Int64{}

	wf := zkafka.NewWorkFactory(client, zkafka.WithLogger(l))
	w := wf.Create(
		zkafka.ConsumerTopicConfig{
			ClientID:  clientID + "-reader",
			Topic:     topic,
			GroupID:   groupID,
			Formatter: zfmt.JSONFmt,
			AdditionalProps: map[string]any{
				"auto.offset.reset": "earliest",
			},
		},
		processor,
		zkafka.CircuitBreakAfter(2),
		zkafka.CircuitBreakFor(500*time.Millisecond),
		zkafka.WithLifecycleHooks(zkafka.LifecycleHooks{
			CircuitBreakerStateChanged: func(ctx context.Context, meta zkafka.LifecycleCircuitBreakerStateChanged) {
				switch meta.To {
				case zkafka.CircuitBreakerStateOpen:
					openedCount.Add(1)
				case zkafka.CircuitBreakerStateClosed:
					closedCount.Add(1)
					// flip back to failing so the breaker can open again on
					// subsequent runs (mirrors the zsqs reference test).
					failing.Store(true)
				}
			},
		}),
	)

	wCtx, wCancel := context.WithCancel(t.Context())
	t.Cleanup(wCancel)

	go func() { _ = w.Run(wCtx, nil) }()

	require.Eventually(t, func() bool { return openedCount.Load() >= 1 },
		20*time.Second, 10*time.Millisecond,
		"CircuitBreakerStateChanged should fire with To=open after consecutive errors")

	// Stop failing so that once the breaker enters half-open, the next
	// successful processor call transitions it back to closed.
	failing.Store(false)

	require.Eventually(t, func() bool { return closedCount.Load() >= 1 },
		20*time.Second, 10*time.Millisecond,
		"CircuitBreakerStateChanged should fire with To=closed after the breaker recovers")
}
