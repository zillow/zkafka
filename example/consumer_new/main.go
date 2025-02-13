package main

import (
	"context"
	_ "embed"
	"fmt"
	"log"
	"time"

	"github.com/zillow/zkafka/v2"
	"github.com/zillow/zkafka/v2/example/common"
	"github.com/zillow/zkafka/v2/test/evolution/avro2"
)

//go:embed schema_2.avsc
var dummyEventSchema2 string

func main() {

	ctx := context.Background()
	bootstrapServer := "localhost:29092"

	client := zkafka.NewClient(zkafka.Config{BootstrapServers: []string{bootstrapServer}}, zkafka.LoggerOption(stdLogger{}))
	defer func() { client.Close() }()

	fmt.Println("Created reader")

	consumerTopicConfig := zkafka.ConsumerTopicConfig{
		ClientID:  common.NewConumser,
		Topic:     common.TempTestingTopicOldToNew,
		Formatter: zkafka.AvroSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL:             "http://localhost:8081",
			Deserialization: zkafka.DeserializationConfig{Schema: dummyEventSchema2},
		},
		GroupID: common.NewConumser,
		AdditionalProps: map[string]any{
			"auto.offset.reset": "earliest",
		},
	}
	reader, err := client.Reader(ctx, consumerTopicConfig)
	if err != nil {
		log.Fatal(err)
	}

	results, err := readMessages(reader, 1)
	if err != nil {
		log.Fatal(err)
	}

	msg2 := <-results
	msg2.Done()
	reader.Close()

	receivedEvt2Schema2 := avro2.Event{}
	if err := msg2.Decode(&receivedEvt2Schema2); err != nil {
		log.Fatal(err)
	}
	log.Println("Received event sucessfully")
}

type stdLogger struct {
	includeDebug bool
}

func (l stdLogger) Debugw(_ context.Context, msg string, keysAndValues ...interface{}) {
	if l.includeDebug {
		log.Printf("Debugw-"+msg, keysAndValues...)
	}
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

func readMessages(reader zkafka.Reader, count int) (<-chan *zkafka.Message, error) {

	responses := make(chan *zkafka.Message, count)

	seen := 0
	for {
		func() {
			ctx := context.Background()
			rmsg, err := reader.Read(ctx)
			defer func() {
				if rmsg == nil {
					return
				}
				rmsg.DoneWithContext(ctx)
			}()
			if err != nil || rmsg == nil {
				return
			}
			responses <- rmsg
			seen++
		}()
		if seen >= count {
			close(responses)
			return responses, nil
		}
	}
}
