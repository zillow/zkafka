package main

import (
	"context"
	_ "embed"
	"log"
	"math/rand"
	"time"

	"github.com/zillow/zkafka"
)

//go:embed dummy_event.avsc
var dummyEventSchema string

func main() {
	ctx := context.Background()
	writer, err := zkafka.NewClient(zkafka.Config{
		BootstrapServers: []string{"localhost:29092"},
	}).Writer(ctx, zkafka.ProducerTopicConfig{
		ClientID:  "example",
		Topic:     "zkafka-example-topic",
		Formatter: zkafka.AvroSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL: "http://localhost:8081",
			Serialization: struct{ AutoRegisterSchemas bool }{
				AutoRegisterSchemas: true,
			},
		},
	})
	randomNames := []string{"stewy", "lydia", "asif", "mike", "justin"}
	if err != nil {
		log.Panic(err)
	}
	for {
		event := DummyEvent{
			IntField:    rand.Intn(100),
			StringField: randomNames[rand.Intn(len(randomNames))],
		}

		resp, err := writer.Write(ctx, event, zkafka.WithAvroSchema(dummyEventSchema))
		//resp, err := writer.Write(ctx, &event)
		if err != nil {
			log.Panic(err)
		}
		log.Printf("resp: %+v\n", resp)
		time.Sleep(time.Second)
	}
}
