package main

import (
	"context"
	_ "embed"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/zillow/zkafka/v2"
)

//go:embed event.avsc
var eventSchema string

// Demonstrates reading from a topic via the zkafka.Work struct which is more convenient, typically, than using the consumer directly
func main() {
	ctx := context.Background()
	client := zkafka.NewClient(zkafka.Config{
		BootstrapServers: []string{"localhost:29092"},
	},
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
		GroupID: uuid.NewString(),
		//GroupID: "zkafka/example/example-consumer",
		Topic: "zkafka-example-topic",
		// The formatter is registered internally to the `zkafka.Message` and used when calling `msg.Decode()`
		// string fmt can be used for both binary and pure strings encoded in the value field of the kafka message. Other options include
		// json, proto, avro, etc.
		Formatter: zkafka.AvroSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL: "http://localhost:8081",
			Deserialization: zkafka.DeserializationConfig{
				// When using avro schema registry, you must specify the schema. In this case,
				// the schema used to generate the golang type is used.
				Schema: eventSchema,
			},
		},
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
	work := wf.CreateWithFunc(topicConfig, Process, zkafka.Speedup(1))
	if err := work.Run(ctx, shutdown); err != nil {
		log.Panic(err)
	}
}

func Process(_ context.Context, msg *zkafka.Message) error {
	// sleep to simulate random amount of work
	time.Sleep(100 * time.Millisecond)

	// The DummyEvent type is generated using `hamba/avro` (see make). This is the preferred generation for
	// `formatter=zkafka.AvroSchemaRegistry` because the underlying deserializer uses the avro tags on the generated struct
	// to properly connect the schema and struct
	event := DummyEvent{}
	err := msg.Decode(&event)
	if err != nil {
		log.Printf("error occurred during processing: %s", err)
		return err
	}

	log.Printf(" offset: %d, partition: %d. event.Age: %d, event.Name %s\n", msg.Offset, msg.Partition, event.IntField, event.StringField)
	return nil
}
