package main

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/zillow/zfmt"
	"github.com/zillow/zkafka/v2"
)

func main() {
	ctx := context.Background()
	writer, err := zkafka.NewClient(zkafka.Config{
		BootstrapServers: []string{"localhost:29092"},
	}).Writer(ctx, zkafka.ProducerTopicConfig{
		ClientID:  "example",
		Topic:     "zkafka-example-topic",
		Formatter: zfmt.JSONFmt,
	})
	randomNames := []string{"stewy", "lydia", "asif", "mike", "justin"}
	if err != nil {
		log.Panic(err)
	}
	for {
		event := DummyEvent{
			Name: randomNames[rand.Intn(len(randomNames))],
			Age:  rand.Intn(100),
		}

		resp, err := writer.Write(ctx, &event)
		if err != nil {
			log.Panic(err)
		}
		log.Printf("resp: %+v\n", resp)
		time.Sleep(time.Second)
	}
}

// DummyEvent is a deserializable struct for producing/consuming kafka message values.
type DummyEvent struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}
