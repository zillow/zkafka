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
			Serialization: zkafka.SerializationConfig{
				// This likely isn't needed in production. A typical workflow involves registering
				// a schema a priori. But for the local example, to save this setup, the flag is set to true
				AutoRegisterSchemas: true,
				// When using avro schema registry, you must specify the schema. In this case,
				// the schema used to generate the golang type is used.
				// The heetch generated struct also embeds the schema as well (and isn't lossy like some of the
				// other generative solutions. For example, one lib didn't include default values), so that could be used as well.
				Schema: dummyEventSchema,
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

		resp, err := writer.Write(ctx, event)
		if err != nil {
			log.Panic(err)
		}
		log.Printf("resp: %+v\n", resp)
		time.Sleep(time.Second)
	}
}
