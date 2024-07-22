package main

import (
	"bytes"
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zillow/zkafka"
	"gitlab.zgtools.net/devex/archetypes/gomods/zfmt"
)

// Demonstrates reading from a topic via the zkafka.Work struct which is more convenient, typically, than using the consumer directly
func main() {
	ctx := context.Background()
	client := zkafka.NewClient(zkafka.Config{
		BootstrapServers: []string{"localhost:9093"},
	},
		zkafka.LoggerOption(stdLogger{}),
	)
	// It's important to close the client after consumption to gracefully leave the consumer group
	// (this commits completed work, and informs the broker that this consumer is leaving the group which yields a faster rebalance)
	defer client.Close()

	topicConfig := zkafka.ConsumerTopicConfig{
		// ClientID is used for caching inside zkafka, and observability within streamz dashboards. But it's not an important
		// part of consumer group semantics. A typical convention is to use the service name executing the kafka worker
		ClientID: "service-name",
		// GroupID is the consumer group. If multiple instances of the same consumer group read messages for the same
		// topic the topic's partitions will be split between the collection. The broker remembers
		// what offset has been committed for a consumer group, and therefore work can be picked up where it was left off
		// across releases
		GroupID: "concierge/example/example-consumery",
		Topic:   "two-multi-partition",
		// The formatter is registered internally to the `zkafka.Message` and used when calling `msg.Decode()`
		// string fmt can be used for both binary and pure strings encoded in the value field of the kafka message. Other options include
		// json, proto, avro, etc.
		Formatter: zfmt.StringFmt,
		AdditionalProps: map[string]any{
			// only important the first time a consumer group connects. Subsequent connections will start
			// consuming messages
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
	// Speedup is used to create multiple processor goroutines. Order is still maintained with this setup by way of `virtual partitions`
	work := wf.Create(topicConfig, &Processor{}, zkafka.Speedup(5))
	work.Run(ctx, shutdown)
}

type Processor struct{}

func (p Processor) Process(_ context.Context, msg *zkafka.Message) error {
	// sleep to simulate random amount of work
	time.Sleep(100 * time.Millisecond)
	var buf bytes.Buffer
	err := msg.Decode(&buf)
	if err != nil {
		return err
	}
	// optionally, if you don't want to use the configured formatter at all, access the kafka message payload bytes directly.
	// The commented out block shows accessing the byte array. In this case we're stringifying the bytes, but this could be json unmarshalled,
	// proto unmarshalled etc., depending on the expected payload
	//data := msg.Value()
	//str := string(data)

	log.Printf("message: %s, offset: %d, partition: %d \n", buf.String(), msg.Offset, msg.Partition)
	return nil
}

type stdLogger struct{}

func (l stdLogger) Debugw(_ context.Context, msg string, keysAndValues ...any) {
	log.Printf("Debugw-"+msg, keysAndValues...)
}

func (l stdLogger) Infow(_ context.Context, msg string, keysAndValues ...any) {
	log.Printf("Infow-"+msg, keysAndValues...)
}

func (l stdLogger) Errorw(_ context.Context, msg string, keysAndValues ...any) {
	log.Printf("Errorw-"+msg, keysAndValues...)
}

func (l stdLogger) Warnw(_ context.Context, msg string, keysAndValues ...any) {
	log.Printf("Warnw-"+msg, keysAndValues...)
}
