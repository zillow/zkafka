package main

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"gitlab.zgtools.net/devex/archetypes/gomods/zfmt"
	"gitlab.zgtools.net/devex/archetypes/gomods/zstreams/v4"
)

func main() {
	ctx := context.Background()
	writer, err := zstreams.NewClient(zstreams.Config{
		BootstrapServers: []string{"localhost:9093"},
	}).Writer(ctx, zstreams.ProducerTopicConfig{
		ClientID:  "example",
		Topic:     "two-multi-partition",
		Formatter: zfmt.JSONFmt,
	})
	if err != nil {
		log.Panic(err)
	}
	for {
		event := DummyEvent{
			Name: uuid.NewString(),
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
