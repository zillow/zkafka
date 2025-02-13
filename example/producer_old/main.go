package main

import (
	"context"
	_ "embed"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/zillow/zkafka/v2"
	"github.com/zillow/zkafka/v2/example/common"
	"github.com/zillow/zkafka/v2/test/evolution/avro1"
	"gitlab.zgtools.net/devex/archetypes/gomods/zcommon"
)

//go:embed schema_1.avsc
var dummyEventSchema1 string

func main() {

	ctx := context.Background()
	bootstrapServer := "localhost:29092"

	err := createTopicWithErr(bootstrapServer, common.TempTestingTopicOldToNew, 1)
	fmt.Printf("Created topic: %s\n", common.TempTestingTopicOldToNew)

	client := zkafka.NewClient(zkafka.Config{BootstrapServers: []string{bootstrapServer}}, zkafka.LoggerOption(stdLogger{}))
	defer func() { client.Close() }()

	fmt.Println("Created writer with auto registered schemas")

	writer2, err := client.Writer(ctx, zkafka.ProducerTopicConfig{
		ClientID:  "myclient",
		Topic:     common.TempTestingTopicOldToNew,
		Formatter: zkafka.AvroSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL: "http://localhost:8081",
			Serialization: zkafka.SerializationConfig{
				AutoRegisterSchemas: true,
				Schema:              dummyEventSchema1,
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	u := "http://localhost:8081"

	evt2 := avro1.Event{
		ID:                     "123",
		DeliveredAtDateTimeUtc: time.Time{},
		EventType:              "created",
		InteractiveContent: zcommon.Ptr([]avro1.InteractiveContentRecord{
			{
				URL: u,
			},
		}),
	}
	_, err = writer2.Write(ctx, &evt2)
	if err != nil {
		log.Fatal(err)
	}
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

func createTopicWithErr(bootstrapServer, topic string, partitions int) error {
	aclient, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrapServer})
	if err != nil {
		return err
	}
	_, err = aclient.CreateTopics(context.Background(), []kafka.TopicSpecification{
		{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		},
	})
	return err
}
