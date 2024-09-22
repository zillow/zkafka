package test

import (
	"context"
	_ "embed"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/zillow/zkafka"
	"github.com/zillow/zkafka/test/evolution/avro1"
	"github.com/zillow/zkafka/test/evolution/avro2"
)

//go:embed evolution/schema_1.avsc
var dummyEventSchema1 string

//go:embed evolution/schema_2.avsc
var dummyEventSchema2 string

const enableSchemaRegistryTest = "ENABLE_SCHEMA_REGISTRY_TESTS"
const enableKafkaBrokerTest = "ENABLE_KAFKA_BROKER_TESTS"

func Test_SchemaRegistry_AutoRegisterSchemas_BackwardCompatibleSchemasCanBeRegisteredAndReadFrom(t *testing.T) {
	checkShouldSkipTest(t, enableKafkaBrokerTest)

	ctx := context.Background()
	topic := "integration-test-topic-2" + uuid.NewString()
	bootstrapServer := getBootstrap()

	createTopic(t, bootstrapServer, topic, 1)
	t.Logf("Created topic: %s", topic)

	groupID := uuid.NewString()

	client := zkafka.NewClient(zkafka.Config{BootstrapServers: []string{bootstrapServer}}, zkafka.LoggerOption(stdLogger{}))
	defer func() { require.NoError(t, client.Close()) }()

	t.Log("Created writer with auto registered schemas")
	writer1, err := client.Writer(ctx, zkafka.ProducerTopicConfig{
		ClientID:  fmt.Sprintf("writer-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.AvroSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL: "mock://",
			Serialization: zkafka.SerializationConfig{
				AutoRegisterSchemas: true,
				Schema:              dummyEventSchema1,
			},
		},
	})
	require.NoError(t, err)

	writer2, err := client.Writer(ctx, zkafka.ProducerTopicConfig{
		ClientID:  fmt.Sprintf("writer-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.AvroSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL: "mock://",
			Serialization: zkafka.SerializationConfig{
				AutoRegisterSchemas: true,
				Schema:              dummyEventSchema2,
			},
		},
	})
	require.NoError(t, err)

	evt1 := avro1.DummyEvent{
		IntField:    int(rand.Int31()),
		DoubleField: rand.Float64(),
		StringField: uuid.NewString(),
		BoolField:   true,
		BytesField:  []byte(uuid.NewString()),
	}
	// write msg1, and msg2
	_, err = writer1.Write(ctx, evt1)
	require.NoError(t, err)

	evt2 := avro2.DummyEvent{
		IntField:            int(rand.Int31()),
		DoubleField:         rand.Float64(),
		StringField:         uuid.NewString(),
		BoolField:           true,
		BytesField:          []byte(uuid.NewString()),
		NewFieldWithDefault: ptr(uuid.NewString()),
	}
	_, err = writer2.Write(ctx, evt2)
	require.NoError(t, err)

	consumerTopicConfig := zkafka.ConsumerTopicConfig{
		ClientID:  fmt.Sprintf("reader-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.AvroSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL: "mock://",
		},
		GroupID: groupID,
		AdditionalProps: map[string]any{
			"auto.offset.reset": "earliest",
		},
	}
	reader, err := client.Reader(ctx, consumerTopicConfig)
	require.NoError(t, err)

	t.Log("Begin reading messages")
	results, err := readMessages(reader, 2)
	require.NoError(t, err)

	msg1 := <-results
	msg2 := <-results
	t.Log("Close reader")

	require.NoError(t, reader.Close())

	receivedEvt1 := avro1.DummyEvent{}
	require.NoError(t, msg1.Decode(&receivedEvt1))
	assertEqual(t, evt1, receivedEvt1)

	receivedEvt2Schema1 := avro1.DummyEvent{}
	require.NoError(t, msg2.Decode(&receivedEvt2Schema1))
	expectedEvt2 := avro1.DummyEvent{
		IntField:    evt2.IntField,
		DoubleField: evt2.DoubleField,
		StringField: evt2.StringField,
		BoolField:   evt2.BoolField,
		BytesField:  evt2.BytesField,
	}
	assertEqual(t, expectedEvt2, receivedEvt2Schema1)

	receivedEvt2Schema2 := avro2.DummyEvent{}
	require.NoError(t, msg2.Decode(&receivedEvt2Schema2))
	assertEqual(t, evt2, receivedEvt2Schema2)
}

func Test_SchemaRegistry_AutoRegisterSchemasFalse_WillNotWriteMessage(t *testing.T) {
	checkShouldSkipTest(t, enableKafkaBrokerTest)

	ctx := context.Background()
	topic := "integration-test-topic-2" + uuid.NewString()
	bootstrapServer := getBootstrap()

	createTopic(t, bootstrapServer, topic, 1)
	t.Logf("Created topic: %s", topic)

	client := zkafka.NewClient(zkafka.Config{BootstrapServers: []string{bootstrapServer}}, zkafka.LoggerOption(stdLogger{}))
	defer func() { require.NoError(t, client.Close()) }()

	t.Log("Created writer with auto registered schemas")
	writer1, err := client.Writer(ctx, zkafka.ProducerTopicConfig{
		ClientID:  fmt.Sprintf("writer-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.AvroSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL: "mock://",
			Serialization: zkafka.SerializationConfig{
				AutoRegisterSchemas: false,
				Schema:              dummyEventSchema1,
			},
		},
	})
	require.NoError(t, err)

	evt1 := avro1.DummyEvent{
		IntField:    int(rand.Int31()),
		DoubleField: rand.Float64(),
		StringField: uuid.NewString(),
		BoolField:   true,
		BytesField:  []byte(uuid.NewString()),
	}
	// write msg1, and msg2
	_, err = writer1.Write(ctx, evt1)
	require.ErrorContains(t, err, "failed to get avro schema by id")
}

// Its possible not specify a schema for your producer.
// In this case, the underlying lib does
func Test_SchemaRegistry_Avro_AutoRegisterSchemas_RequiresSchemaSpecification(t *testing.T) {
	checkShouldSkipTest(t, enableKafkaBrokerTest)

	ctx := context.Background()
	topic := "integration-test-topic-2" + uuid.NewString()
	bootstrapServer := getBootstrap()

	createTopic(t, bootstrapServer, topic, 1)
	t.Logf("Created topic: %s", topic)

	client := zkafka.NewClient(zkafka.Config{BootstrapServers: []string{bootstrapServer}}, zkafka.LoggerOption(stdLogger{}))
	defer func() { require.NoError(t, client.Close()) }()

	t.Log("Created writer with auto registered schemas")
	writer1, err := client.Writer(ctx, zkafka.ProducerTopicConfig{
		ClientID:  fmt.Sprintf("writer-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.AvroSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL: "mock://",
			Serialization: zkafka.SerializationConfig{
				AutoRegisterSchemas: true,
				// don't specify schema uses implicit handling
				Schema: "",
			},
		},
	})
	require.NoError(t, err)

	evt1 := avro1.DummyEvent{
		IntField:    int(rand.Int31()),
		DoubleField: rand.Float64(),
		StringField: uuid.NewString(),
		BoolField:   true,
		BytesField:  []byte(uuid.NewString()),
	}
	// write msg1, and msg2
	_, err = writer1.Write(ctx, evt1)
	require.ErrorContains(t, err, "avro schema is required for schema registry formatter")
}

func Test_SchemaNotRegistered_ImpactToWorker(t *testing.T) {
	require.Fail(t, "implement")
}
func Test_JsonSchemaRegistry(t *testing.T) {
	require.Fail(t, "implement")
}

//func Test_AlternateSubjectNamingStrategy(t *testing.T) {
//	require.Fail(t, "implement")
//}

func checkShouldSkipTest(t *testing.T, flags ...string) {
	t.Helper()
	for _, flag := range flags {
		if os.Getenv(flag) != "true" {
			t.Skipf("Skipping test. To execute. Set envvar '%s' to true", flag)
		}
	}
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
