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
	"github.com/zillow/zkafka/test/heetch1"
	"github.com/zillow/zkafka/test/heetch2"
)

//go:embed dummy_event_1.avsc
var dummyEventSchema1 string

//go:embed dummy_event_2.avsc
var dummyEventSchema2 string

const enableSchemaRegistryTest = "ENABLE_SCHEMA_REGISTRY_TESTS"
const enableKafkaBrokerTest = "ENABLE_KAFKA_BROKER_TESTS"

// Test_AutoRegisterSchemas_BackwardCompatibleSchemasCanBeRegisteredAndReadFrom tests
// the `avro_confluent` formatter which uses schema registry.
//
// two schemas exists, which are backwards compatible with one another.
// The default behavior of the confluent-kafka-go doesn't handle this well, since the new field in schema2
//
//	which has a default value and is nullable has the default value omitted in schema registration and is therefore
//
// found to be incompatible. Auto registration isn't typically used in production environments,
// but this behavior is still problematic because the implicit schema resolution is used to communicate with schema registry
// and determine the appropriate schemaID to embed. The omitted default means the correct schema isn't found.
//
// The two message sucesfully writing means the two schemas are registered.
// We then test we can use the confluent deserializer to decode the messages. For both schema1 and schema2.
// This confirms that backwards/forward compatible evolution is possible and old schemas can still read messages from new.
func Test_AutoRegisterSchemas_BackwardCompatibleSchemasCanBeRegisteredAndReadFrom(t *testing.T) {
	checkShouldSkipTest(t, enableSchemaRegistryTest)

	ctx := context.Background()
	topic := "integration-test-topic-2" + uuid.NewString()
	bootstrapServer := getBootstrap()

	createTopic(t, bootstrapServer, topic, 1)
	t.Logf("Created topic: %s", topic)

	groupID := uuid.NewString()

	client := zkafka.NewClient(zkafka.Config{BootstrapServers: []string{bootstrapServer}}, zkafka.LoggerOption(stdLogger{}))
	defer func() { require.NoError(t, client.Close()) }()

	t.Log("Created writer with auto registered schemas")
	writer, err := client.Writer(ctx, zkafka.ProducerTopicConfig{
		ClientID:  fmt.Sprintf("writer-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.AvroConfluentFmt,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL: "http://localhost:8081",
			Serialization: struct{ AutoRegisterSchemas bool }{
				AutoRegisterSchemas: true,
			},
		},
	})
	require.NoError(t, err)

	evt1 := heetch1.DummyEvent{
		IntField:    int(rand.Int31()),
		DoubleField: rand.Float64(),
		StringField: uuid.NewString(),
		BoolField:   true,
		BytesField:  []byte(uuid.NewString()),
	}
	// write msg1, and msg2
	_, err = writer.Write(ctx, evt1, zkafka.WithAvroSchema(dummyEventSchema1))
	require.NoError(t, err)

	evt2 := heetch2.DummyEvent{
		IntField:            int(rand.Int31()),
		DoubleField:         rand.Float64(),
		StringField:         uuid.NewString(),
		BoolField:           true,
		BytesField:          []byte(uuid.NewString()),
		NewFieldWithDefault: ptr(uuid.NewString()),
	}
	_, err = writer.Write(ctx, evt2, zkafka.WithAvroSchema(dummyEventSchema2))
	require.NoError(t, err)

	consumerTopicConfig := zkafka.ConsumerTopicConfig{
		ClientID:  fmt.Sprintf("reader-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.AvroConfluentFmt,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL: "http://localhost:8081",
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

	receivedEvt1 := heetch1.DummyEvent{}
	require.NoError(t, msg1.Decode(&receivedEvt1))
	assertEqual(t, evt1, receivedEvt1)

	receivedEvt2Schema1 := heetch1.DummyEvent{}
	require.NoError(t, msg2.Decode(&receivedEvt2Schema1))
	expectedEvt2 := heetch1.DummyEvent{
		IntField:    evt2.IntField,
		DoubleField: evt2.DoubleField,
		StringField: evt2.StringField,
		BoolField:   evt2.BoolField,
		BytesField:  evt2.BytesField,
	}
	assertEqual(t, expectedEvt2, receivedEvt2Schema1)

	receivedEvt2Schema2 := heetch2.DummyEvent{}
	require.NoError(t, msg2.Decode(&receivedEvt2Schema2))
	assertEqual(t, evt2, receivedEvt2Schema2)
}

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
