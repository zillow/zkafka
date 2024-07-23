package main

import (
	"context"
	"fmt"
	"log"

	"github.com/zillow/zfmt"
	"github.com/zillow/zkafka"
)

func main() {
	// configure broker connectivity options with zkafka.Config
	cfg := zkafka.Config{
		BootstrapServers: []string{"localhost:9092"},
	}

	// configure consumer options  with zkafka.ConsumerTopicConfig. See zkafka for full option values
	topicConfig := zkafka.ConsumerTopicConfig{
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
	reader, err := zkafka.NewClient(cfg).Reader(ctx, topicConfig)
	if err != nil {
		log.Fatal(err)
	}
	for {
		// Poll for 1 message for up to 10 seconds. Return nil if no messages available.
		// To continually poll a kafka message, we suggest using zkafka in conjunction with zwork. This offers a
		// good mechanism for processing messages using stateless messaging semantics using golang
		msg, err := reader.Read(ctx)
		if err != nil {
			log.Fatal(err)
		}
		if msg == nil {
			log.Printf("No messages available")
			return
		}

		// zkafka.Message (the type of msg variable) wraps a kafka message exposing higher level APIs for interacting with the data.
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
