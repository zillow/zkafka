package main

import (
	"context"
	"fmt"
	"log"

	"gitlab.zgtools.net/devex/archetypes/gomods/zfmt"
	"gitlab.zgtools.net/devex/archetypes/gomods/zstreams/v4"
)

func main() {
	// configure broker connectivity options with zstreams.Config
	cfg := zstreams.Config{
		BootstrapServers: []string{"localhost:9093"},
	}

	// configure consumer options  with zstreams.ConsumerTopicConfig. See zstreams for full option values
	topicConfig := zstreams.ConsumerTopicConfig{
		ClientID: "xxx",
		GroupID:  "golang-example-consumer-3",
		Topic:    "two-multi-partition",
		// defaults to 1 second. Use 10 seconds for example to give time to establish connection
		ReadTimeoutMillis: ptr(10000),
		// Specify the formatter used to deserialize the contents of kafka message
		Formatter: zfmt.JSONFmt,
		AdditionalProps: map[string]any{
			"auto.offset.reset": "earliest",
		},
	}

	ctx := context.Background()
	// Create a reader using Config and ConsumerTopicConfig
	reader, err := zstreams.NewClient(cfg).Reader(ctx, topicConfig)
	if err != nil {
		log.Fatal(err)
	}
	for {
		// Poll for 1 message for up to 10 seconds. Return nil if no messages available.
		// To continually poll a kafka message, we suggest using zstreams in conjunction with zwork. This offers a
		// good mechanism for processing messages using stateless messaging semantics using golang
		msg, err := reader.Read(ctx)
		if err != nil {
			log.Fatal(err)
		}
		if msg == nil {
			log.Printf("No messages available")
			return
		}

		// zstreams.Message (the type of msg variable) wraps a kafka message exposing higher level APIs for interacting with the data.
		// This includes a decode method for easily
		// deserializing the kafka value byte array. In this case, we're using the JSONFmt (specified in TopicConfig).
		item := DummyEvent{}
		if err = msg.Decode(&item); err != nil {
			log.Fatal(err)
		}

		// print out the contents of kafka message used to hydrate the DummyEvent struct
		fmt.Printf("dummy event %+v\n", item)

		// call msg.Done to commit the work with the kafka broker
		msg.Done()
	}
}

// DummyEvent is a deserializable struct for producing/consuming kafka message values.
type DummyEvent struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func ptr[T any](v T) *T {
	return &v
}
