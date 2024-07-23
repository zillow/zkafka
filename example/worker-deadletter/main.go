package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zillow/zkafka"
)

// Demonstrates reading from a topic via the zkafka.Work struct which is more convenient, typically, than using the consumer directly
func main() {
	ctx := context.Background()
	client := zkafka.NewClient(zkafka.Config{
		BootstrapServers: []string{"localhost:29092"},
	},
		zkafka.LoggerOption(stdLogger{}),
	)
	// It's important to close the client after consumption to gracefully leave the consumer group
	// (this commits completed work, and informs the broker that this consumer is leaving the group which yields a faster rebalance)
	defer client.Close()

	readTimeoutMillis := 10000
	topicConfig := zkafka.ConsumerTopicConfig{
		// ClientID is used for caching inside zkafka, and observability within streamz dashboards. But it's not an important
		// part of consumer group semantics. A typical convention is to use the service name executing the kafka worker
		ClientID: "service-name",
		// GroupID is the consumer group. If multiple instances of the same consumer group read messages for the same
		// topic the topic's partitions will be split between the collection. The broker remembers
		// what offset has been committed for a consumer group, and therefore work can be picked up where it was left off
		// across releases
		GroupID: "zkafka/example/example-consumer",
		Topic:   "zkafka-example-topic",
		// Controls how long ReadMessage() wait in work before returning a nil message. The default is 1s, but is increased in this example
		// to reduce the number of debug logs which come when a nil message is returned
		ReadTimeoutMillis: &readTimeoutMillis,
		// When DeadLetterTopicConfig is specified a dead letter topic will be configured and written to
		// when a processing error occurs.
		DeadLetterTopicConfig: &zkafka.ProducerTopicConfig{
			Topic: "zkafka-example-deadletter-topic",
		},
		AdditionalProps: map[string]any{
			// only important the first time a consumer group connects.
			"auto.offset.reset": "earliest",
		},
	}
	// optionally set up a channel to signal when worker shutdown should occur.
	// A nil channel is also acceptable, but this example demonstrates how to make utility of the signal.
	// The channel should be closed, instead of simply written to, to properly broadcast to the separate worker threads.
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, os.Interrupt, syscall.SIGTERM)
	shutdown := make(chan struct{})

	go func() {
		<-stopCh
		close(shutdown)
	}()

	wf := zkafka.NewWorkFactory(client)
	// Register a processor which is executed per message.
	work := wf.Create(topicConfig, &Processor{})
	if err := work.Run(ctx, shutdown); err != nil {
		log.Panic(err)
	}
}

type Processor struct{}

func (p Processor) Process(_ context.Context, msg *zkafka.Message) error {
	// Processing errors result in the message being written to the configured dead letter topic (DLT).
	// Any error object works, but finer grained controlled cn be accomplished by returning a
	// `zkafka.ProcessError`. In this example, we control the behavior of the circuit breaker and can optionally
	// skip writing the DLT (this example doesn't opt to do that)
	//
	// Because debug logging is on, the producer log (for when a message is written to the DLT) will show in std out
	return zkafka.ProcessError{
		Err:                 errors.New("processing failed"),
		DisableCircuitBreak: true,
		DisableDLTWrite:     false,
	}
}

type stdLogger struct {
}

func (l stdLogger) Debugw(_ context.Context, msg string, keysAndValues ...interface{}) {
	log.Printf("Debugw-"+msg, keysAndValues...)
}

func (l stdLogger) Infow(_ context.Context, msg string, keysAndValues ...interface{}) {
	log.Printf("Infow-"+msg, keysAndValues...)
}

func (l stdLogger) Errorw(_ context.Context, msg string, keysAndValues ...interface{}) {
	log.Printf("Errorw-"+msg, keysAndValues...)
}

func (l stdLogger) Warnw(_ context.Context, msg string, keysAndValues ...interface{}) {
	prefix := fmt.Sprintf("Warnw-%s-"+msg, time.Now().Format(time.RFC3339Nano))
	log.Printf(prefix, keysAndValues...)
}
