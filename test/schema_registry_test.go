package test

import (
	"context"
	_ "embed"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/zillow/zfmt"
	"github.com/zillow/zkafka/v2"
	"github.com/zillow/zkafka/v2/test/evolution/avro1"
	avro1x "github.com/zillow/zkafka/v2/test/evolution/avro1x"
	"github.com/zillow/zkafka/v2/test/evolution/json1"
	"github.com/zillow/zkafka/v2/test/evolution/proto1"
)

//go:embed evolution/schema_1.avsc
var dummyEventSchema1 string

//go:embed evolution/schema_2.avsc
var dummyEventSchema2 string

const enableSchemaRegistryTest = "ENABLE_SCHEMA_REGISTRY_TESTS"
const enableKafkaBrokerTest = "ENABLE_KAFKA_BROKER_TESTS"

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

	evt1 := avro1.Event{}

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

	evt1 := avro1.Event{}

	_, err = writer1.Write(ctx, evt1)
	require.ErrorContains(t, err, "avro schema is required for schema registry formatter")
}

// Test_SchemaNotRegistered_ResultsInWorkerDecodeError demonstrates the behavior when a worker reads
// a message for a schema that doesn't exist in shcema registry. This test shows that such a situation would result in a decode error

func Test_SchemaNotRegistered_ResultsInWorkerDecodeError(t *testing.T) {
	checkShouldSkipTest(t, enableKafkaBrokerTest)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	topic := "integration-test-topic-2" + uuid.NewString()
	bootstrapServer := getBootstrap()

	createTopic(t, bootstrapServer, topic, 1)
	t.Logf("Created topic: %s", topic)

	groupID := uuid.NewString()

	client := zkafka.NewClient(zkafka.Config{BootstrapServers: []string{bootstrapServer}}, zkafka.LoggerOption(stdLogger{}))
	defer func() { require.NoError(t, client.Close()) }()

	t.Log("Created writer - no schema registration")
	writer1, err := client.Writer(ctx, zkafka.ProducerTopicConfig{
		ClientID:  fmt.Sprintf("writer-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zfmt.AvroSchemaFmt,
		SchemaID:  1,
	})
	require.NoError(t, err)

	evt1 := avro1x.Event{}

	_, err = writer1.Write(ctx, evt1)
	require.NoError(t, err)

	consumerTopicConfig := zkafka.ConsumerTopicConfig{
		ClientID:  fmt.Sprintf("reader-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.AvroSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL: "mock://",
			Deserialization: zkafka.DeserializationConfig{
				Schema: dummyEventSchema1,
			},
		},
		GroupID: groupID,
		AdditionalProps: map[string]any{
			"auto.offset.reset": "earliest",
		},
	}
	var gotErr error
	wf := zkafka.NewWorkFactory(client)
	w := wf.CreateWithFunc(consumerTopicConfig, func(_ context.Context, msg *zkafka.Message) error {
		defer cancel()
		gotErr = msg.Decode(&avro1.Event{})
		return gotErr
	})

	t.Log("Begin reading messages")
	err = w.Run(ctx, nil)
	require.NoError(t, err)
	require.ErrorContains(t, gotErr, "Subject Not Found")
}

func Test_SchemaRegistry_Avro_SubjectNameSpecification(t *testing.T) {
	checkShouldSkipTest(t, enableKafkaBrokerTest)

	ctx := context.Background()
	topic := "integration-test-topic-2" + uuid.NewString()
	bootstrapServer := getBootstrap()

	createTopic(t, bootstrapServer, topic, 1)
	t.Logf("Created topic: %s", topic)

	groupID := uuid.NewString()

	client := zkafka.NewClient(zkafka.Config{BootstrapServers: []string{bootstrapServer}}, zkafka.LoggerOption(stdLogger{}))
	defer func() { require.NoError(t, client.Close()) }()

	subjName := uuid.NewString()
	t.Log("Created writer with auto registered schemas")
	writer1, err := client.Writer(ctx, zkafka.ProducerTopicConfig{
		ClientID:  fmt.Sprintf("writer-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.AvroSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL:         "mock://",
			SubjectName: subjName,
			Serialization: zkafka.SerializationConfig{
				AutoRegisterSchemas: true,
				Schema:              dummyEventSchema1,
			},
		},
	})
	require.NoError(t, err)

	evt1 := avro1.Event{}

	// write msg1, and msg2
	_, err = writer1.Write(ctx, evt1)
	require.NoError(t, err)

	consumerTopicConfig := zkafka.ConsumerTopicConfig{
		ClientID:  fmt.Sprintf("reader-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.AvroSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL:         "mock://",
			SubjectName: subjName,
			Deserialization: zkafka.DeserializationConfig{
				Schema: dummyEventSchema1,
			},
		},
		GroupID: groupID,
		AdditionalProps: map[string]any{
			"auto.offset.reset": "earliest",
		},
	}
	reader, err := client.Reader(ctx, consumerTopicConfig)
	require.NoError(t, err)

	t.Log("Begin reading messages")
	results, err := readMessages(reader, 1)
	require.NoError(t, err)

	msg1 := <-results
	t.Log("Close reader")

	require.NoError(t, reader.Close())

	receivedEvt1 := avro1.Event{}
	require.NoError(t, msg1.Decode(&receivedEvt1))
	assertEqual(t, evt1, receivedEvt1)
}

func Test_SchemaRegistry_Proto_SubjectNameSpecification(t *testing.T) {
	checkShouldSkipTest(t, enableKafkaBrokerTest)

	ctx := context.Background()
	topic := "integration-test-topic-2" + uuid.NewString()
	bootstrapServer := getBootstrap()

	createTopic(t, bootstrapServer, topic, 1)
	t.Logf("Created topic: %s", topic)

	groupID := uuid.NewString()

	client := zkafka.NewClient(zkafka.Config{BootstrapServers: []string{bootstrapServer}}, zkafka.LoggerOption(stdLogger{}))
	defer func() { require.NoError(t, client.Close()) }()

	subjName := uuid.NewString()
	t.Log("Created writer with auto registered schemas")
	writer1, err := client.Writer(ctx, zkafka.ProducerTopicConfig{
		ClientID:  fmt.Sprintf("writer-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.ProtoSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL:         "mock://",
			SubjectName: subjName,
			Serialization: zkafka.SerializationConfig{
				AutoRegisterSchemas: true,
				Schema:              dummyEventSchema1,
			},
		},
	})
	require.NoError(t, err)

	evt1 := &proto1.DummyEvent{
		IntField:    rand.Int63(),
		DoubleField: rand.Float32(),
		StringField: uuid.NewString(),
		BoolField:   true,
		BytesField:  []byte(uuid.NewString()),
	}
	_, err = writer1.Write(ctx, evt1)
	require.NoError(t, err)

	consumerTopicConfig := zkafka.ConsumerTopicConfig{
		ClientID:  fmt.Sprintf("reader-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.ProtoSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL:         "mock://",
			SubjectName: subjName,
			Deserialization: zkafka.DeserializationConfig{
				Schema: dummyEventSchema1,
			},
		},
		GroupID: groupID,
		AdditionalProps: map[string]any{
			"auto.offset.reset": "earliest",
		},
	}
	reader, err := client.Reader(ctx, consumerTopicConfig)
	require.NoError(t, err)

	t.Log("Begin reading messages")
	results, err := readMessages(reader, 1)
	require.NoError(t, err)

	msg1 := <-results
	t.Log("Close reader")

	require.NoError(t, reader.Close())

	receivedEvt1 := &proto1.DummyEvent{}
	require.NoError(t, msg1.Decode(receivedEvt1))
	assertEqual(t, evt1, receivedEvt1, cmpopts.IgnoreUnexported(proto1.DummyEvent{}))
}

func Test_SchemaRegistry_Json_SubjectNameSpecification(t *testing.T) {
	checkShouldSkipTest(t, enableKafkaBrokerTest)

	ctx := context.Background()
	topic := "integration-test-topic-2" + uuid.NewString()
	bootstrapServer := getBootstrap()

	createTopic(t, bootstrapServer, topic, 1)
	t.Logf("Created topic: %s", topic)

	groupID := uuid.NewString()

	client := zkafka.NewClient(zkafka.Config{BootstrapServers: []string{bootstrapServer}}, zkafka.LoggerOption(stdLogger{}))
	defer func() { require.NoError(t, client.Close()) }()

	subjName := uuid.NewString()
	t.Log("Created writer with auto registered schemas")
	writer1, err := client.Writer(ctx, zkafka.ProducerTopicConfig{
		ClientID:  fmt.Sprintf("writer-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.JSONSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL:         "mock://",
			SubjectName: subjName,
			Serialization: zkafka.SerializationConfig{
				AutoRegisterSchemas: true,
				Schema:              dummyEventSchema1,
			},
		},
	})
	require.NoError(t, err)

	evt1 := &json1.DummyEvent{
		IntField:    rand.Int63(),
		DoubleField: rand.Float32(),
		StringField: uuid.NewString(),
		BoolField:   true,
		BytesField:  []byte(uuid.NewString()),
	}
	_, err = writer1.Write(ctx, evt1)
	require.NoError(t, err)

	consumerTopicConfig := zkafka.ConsumerTopicConfig{
		ClientID:  fmt.Sprintf("reader-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.JSONSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL:         "mock://",
			SubjectName: subjName,
			Deserialization: zkafka.DeserializationConfig{
				Schema: dummyEventSchema1,
			},
		},
		GroupID: groupID,
		AdditionalProps: map[string]any{
			"auto.offset.reset": "earliest",
		},
	}
	reader, err := client.Reader(ctx, consumerTopicConfig)
	require.NoError(t, err)

	t.Log("Begin reading messages")
	results, err := readMessages(reader, 1)
	require.NoError(t, err)

	msg1 := <-results
	t.Log("Close reader")

	require.NoError(t, reader.Close())

	receivedEvt1 := &json1.DummyEvent{}
	require.NoError(t, msg1.Decode(&receivedEvt1))
	assertEqual(t, evt1, receivedEvt1)
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
