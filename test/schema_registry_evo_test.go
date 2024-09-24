// Build tag is added here because the proto evolution test creates a package loading runtime error.
// In the pipeline, this error is suppressed  with an envvar. However, this repo wants to remain idiomatic
// and devs should be able to run `go test ./...` without the package loading runtime error.
//go:build evolution_test
// +build evolution_test

package test

import (
	"context"
	_ "embed"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/zillow/zkafka"
	"github.com/zillow/zkafka/test/evolution/avro1"
	"github.com/zillow/zkafka/test/evolution/avro2"
	"github.com/zillow/zkafka/test/evolution/json1"
	"github.com/zillow/zkafka/test/evolution/json2"
	"github.com/zillow/zkafka/test/evolution/proto1"
	"github.com/zillow/zkafka/test/evolution/proto2"
)

// Test_SchemaRegistryReal_Avro_AutoRegisterSchemas_BackwardCompatibleSchemasCanBeRegisteredAndReadFrom tests
// the `avro_schema_registry` formatter which uses schema registry.
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
// The two message successfully writing means the two schemas are registered.
// We then test we can use the confluent deserializer to decode the messages. For both schema1 and schema2.
// This confirms that backwards/forward compatible evolution is possible and old schemas can still read messages from new.
func Test_SchemaRegistryReal_Avro_AutoRegisterSchemas_BackwardCompatibleSchemasCanBeRegisteredAndReadFrom(t *testing.T) {
	checkShouldSkipTest(t, enableKafkaBrokerTest, enableSchemaRegistryTest)

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
			URL: "http://localhost:8081",
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
			URL: "http://localhost:8081",
			Serialization: zkafka.SerializationConfig{
				AutoRegisterSchemas: true,
				Schema:              dummyEventSchema2,
			},
		},
	})
	require.NoError(t, err)
	listingID := uuid.NewString()

	evt1 := avro1.Event{
		ID:                     listingID,
		DeliveredAtDateTimeUtc: time.Now().UTC().Truncate(time.Millisecond),
		EventType:              "created",
	}
	// write msg1, and msg2
	_, err = writer1.Write(ctx, &evt1)
	require.NoError(t, err)

	listingID2 := uuid.NewString()

	evt2 := avro2.Event{
		ID:                     listingID2,
		DeliveredAtDateTimeUtc: time.Now().UTC().Truncate(time.Millisecond),
		EventType:              "created",
	}
	_, err = writer2.Write(ctx, &evt2)
	require.NoError(t, err)

	consumerTopicConfig := zkafka.ConsumerTopicConfig{
		ClientID:  fmt.Sprintf("reader-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.AvroSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL:             "http://localhost:8081",
			Deserialization: zkafka.DeserializationConfig{Schema: dummyEventSchema1},
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

	receivedEvt1 := avro1.Event{}
	require.NoError(t, msg1.Decode(&receivedEvt1))
	assertEqual(t, receivedEvt1, evt1)

	receivedEvt2Schema1 := avro1.Event{}
	require.NoError(t, msg2.Decode(&receivedEvt2Schema1))
	expectedEvt2 := avro1.Event{
		ID:                     evt2.ID,
		DeliveredAtDateTimeUtc: evt2.DeliveredAtDateTimeUtc,
		EventType:              evt2.EventType,
	}
	assertEqual(t, expectedEvt2, receivedEvt2Schema1)

	receivedEvt2Schema2 := avro2.Event{}
	require.NoError(t, msg2.Decode(&receivedEvt2Schema2))
	assertEqual(t, evt2, receivedEvt2Schema2)
}

func Test_SchemaRegistryReal_Proto_AutoRegisterSchemas_BackwardCompatibleSchemasCanBeRegisteredAndReadFrom(t *testing.T) {
	checkShouldSkipTest(t, enableKafkaBrokerTest, enableSchemaRegistryTest)

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
		Formatter: zkafka.ProtoSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL: "http://localhost:8081",
			Serialization: zkafka.SerializationConfig{
				AutoRegisterSchemas: true,
				//Schema:              dummyEventSchema1,
			},
		},
	})
	require.NoError(t, err)

	writer2, err := client.Writer(ctx, zkafka.ProducerTopicConfig{
		ClientID:  fmt.Sprintf("writer-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.ProtoSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL: "http://localhost:8081",
			Serialization: zkafka.SerializationConfig{
				AutoRegisterSchemas: true,
				//Schema:              dummyEventSchema2,
			},
		},
	})
	require.NoError(t, err)

	evt1 := proto1.DummyEvent{
		IntField:    rand.Int63(),
		DoubleField: rand.Float32(),
		StringField: uuid.NewString(),
		BoolField:   true,
		BytesField:  []byte(uuid.NewString()),
	}
	// write msg1, and msg2
	_, err = writer1.Write(ctx, &evt1)
	require.NoError(t, err)

	evt2 := proto2.DummyEvent{
		IntField:    rand.Int63(),
		DoubleField: rand.Float32(),
		StringField: uuid.NewString(),
		BoolField:   true,
		BytesField:  []byte(uuid.NewString()),
		NewField:    uuid.NewString(),
	}
	_, err = writer2.Write(ctx, &evt2)
	require.NoError(t, err)

	consumerTopicConfig := zkafka.ConsumerTopicConfig{
		ClientID:  fmt.Sprintf("reader-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.ProtoSchemaRegistry,
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

	receivedEvt1 := proto1.DummyEvent{}
	require.NoError(t, msg1.Decode(&receivedEvt1))
	assertEqual(t, evt1, receivedEvt1, cmpopts.IgnoreUnexported(proto1.DummyEvent{}))

	receivedEvt2Schema1 := proto1.DummyEvent{}
	require.NoError(t, msg2.Decode(&receivedEvt2Schema1))
	expectedEvt2 := proto1.DummyEvent{
		IntField:    evt2.IntField,
		DoubleField: evt2.DoubleField,
		StringField: evt2.StringField,
		BoolField:   evt2.BoolField,
		BytesField:  evt2.BytesField,
	}
	assertEqual(t, expectedEvt2, receivedEvt2Schema1, cmpopts.IgnoreUnexported(proto1.DummyEvent{}))

	receivedEvt2Schema2 := proto2.DummyEvent{}
	require.NoError(t, msg2.Decode(&receivedEvt2Schema2))
	assertEqual(t, evt2, receivedEvt2Schema2, cmpopts.IgnoreUnexported(proto2.DummyEvent{}))
}

func Test_SchemaRegistryReal_JSON_AutoRegisterSchemas_BackwardCompatibleSchemasCanBeRegisteredAndReadFrom(t *testing.T) {
	checkShouldSkipTest(t, enableKafkaBrokerTest, enableSchemaRegistryTest)

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
		Formatter: zkafka.JSONSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL: "http://localhost:8081",
			Serialization: zkafka.SerializationConfig{
				AutoRegisterSchemas: true,
			},
		},
	})
	require.NoError(t, err)

	writer2, err := client.Writer(ctx, zkafka.ProducerTopicConfig{
		ClientID:  fmt.Sprintf("writer-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.JSONSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL: "http://localhost:8081",
			Serialization: zkafka.SerializationConfig{
				AutoRegisterSchemas: true,
			},
		},
	})
	require.NoError(t, err)

	evt1 := json1.DummyEvent{
		IntField:    rand.Int63(),
		DoubleField: rand.Float32(),
		StringField: uuid.NewString(),
		BoolField:   true,
		BytesField:  []byte(uuid.NewString()),
	}
	// write msg1, and msg2
	_, err = writer1.Write(ctx, &evt1)
	require.NoError(t, err)

	evt2 := json2.DummyEvent{
		IntField:    rand.Int63(),
		DoubleField: rand.Float32(),
		StringField: uuid.NewString(),
		BoolField:   true,
		BytesField:  []byte(uuid.NewString()),
		NewField:    uuid.NewString(),
	}
	_, err = writer2.Write(ctx, &evt2)
	require.NoError(t, err)

	consumerTopicConfig := zkafka.ConsumerTopicConfig{
		ClientID:  fmt.Sprintf("reader-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.JSONSchemaRegistry,
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

	receivedEvt1 := json1.DummyEvent{}
	require.NoError(t, msg1.Decode(&receivedEvt1))
	assertEqual(t, evt1, receivedEvt1, cmpopts.IgnoreUnexported(json1.DummyEvent{}))

	receivedEvt2Schema1 := json1.DummyEvent{}
	require.NoError(t, msg2.Decode(&receivedEvt2Schema1))
	expectedEvt2 := json1.DummyEvent{
		IntField:    evt2.IntField,
		DoubleField: evt2.DoubleField,
		StringField: evt2.StringField,
		BoolField:   evt2.BoolField,
		BytesField:  evt2.BytesField,
	}
	assertEqual(t, expectedEvt2, receivedEvt2Schema1, cmpopts.IgnoreUnexported(json1.DummyEvent{}))

	receivedEvt2Schema2 := json2.DummyEvent{}
	require.NoError(t, msg2.Decode(&receivedEvt2Schema2))
	assertEqual(t, evt2, receivedEvt2Schema2, cmpopts.IgnoreUnexported(json2.DummyEvent{}))
}

func Test_SchemaRegistry_Proto_AutoRegisterSchemas_BackwardCompatibleSchemasCanBeRegisteredAndReadFrom(t *testing.T) {
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
		Formatter: zkafka.ProtoSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL: "mock://",
			Serialization: zkafka.SerializationConfig{
				AutoRegisterSchemas: true,
			},
		},
	})
	require.NoError(t, err)

	writer2, err := client.Writer(ctx, zkafka.ProducerTopicConfig{
		ClientID:  fmt.Sprintf("writer-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.ProtoSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL: "mock://",
			Serialization: zkafka.SerializationConfig{
				AutoRegisterSchemas: true,
			},
		},
	})
	require.NoError(t, err)

	evt1 := proto1.DummyEvent{
		IntField:    rand.Int63(),
		DoubleField: rand.Float32(),
		StringField: uuid.NewString(),
		BoolField:   true,
		BytesField:  []byte(uuid.NewString()),
	}
	// write msg1, and msg2
	_, err = writer1.Write(ctx, &evt1)
	require.NoError(t, err)

	evt2 := proto2.DummyEvent{
		IntField:    rand.Int63(),
		DoubleField: rand.Float32(),
		StringField: uuid.NewString(),
		BoolField:   true,
		BytesField:  []byte(uuid.NewString()),
		NewField:    uuid.NewString(),
	}
	_, err = writer2.Write(ctx, &evt2)
	require.NoError(t, err)

	consumerTopicConfig := zkafka.ConsumerTopicConfig{
		ClientID:  fmt.Sprintf("reader-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.ProtoSchemaRegistry,
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

	receivedEvt1 := proto1.DummyEvent{}
	require.NoError(t, msg1.Decode(&receivedEvt1))
	assertEqual(t, evt1, receivedEvt1, cmpopts.IgnoreUnexported(proto1.DummyEvent{}))

	receivedEvt2Schema1 := proto1.DummyEvent{}
	require.NoError(t, msg2.Decode(&receivedEvt2Schema1))
	expectedEvt2 := proto1.DummyEvent{
		IntField:    evt2.IntField,
		DoubleField: evt2.DoubleField,
		StringField: evt2.StringField,
		BoolField:   evt2.BoolField,
		BytesField:  evt2.BytesField,
	}
	assertEqual(t, expectedEvt2, receivedEvt2Schema1, cmpopts.IgnoreUnexported(proto1.DummyEvent{}))

	receivedEvt2Schema2 := proto2.DummyEvent{}
	require.NoError(t, msg2.Decode(&receivedEvt2Schema2))
	assertEqual(t, evt2, receivedEvt2Schema2, cmpopts.IgnoreUnexported(proto2.DummyEvent{}))
}

func Test_SchemaRegistry_JSON_AutoRegisterSchemas_BackwardCompatibleSchemasCanBeRegisteredAndReadFrom(t *testing.T) {
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
		Formatter: zkafka.JSONSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL: "mock://",
			Serialization: zkafka.SerializationConfig{
				AutoRegisterSchemas: true,
			},
		},
	})
	require.NoError(t, err)

	writer2, err := client.Writer(ctx, zkafka.ProducerTopicConfig{
		ClientID:  fmt.Sprintf("writer-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.JSONSchemaRegistry,
		SchemaRegistry: zkafka.SchemaRegistryConfig{
			URL: "mock://",
			Serialization: zkafka.SerializationConfig{
				AutoRegisterSchemas: true,
			},
		},
	})
	require.NoError(t, err)

	evt1 := json1.DummyEvent{
		IntField:    rand.Int63(),
		DoubleField: rand.Float32(),
		StringField: uuid.NewString(),
		BoolField:   true,
		BytesField:  []byte(uuid.NewString()),
	}
	_, err = writer1.Write(ctx, &evt1)
	require.NoError(t, err)

	evt2 := json2.DummyEvent{
		IntField:    rand.Int63(),
		DoubleField: rand.Float32(),
		StringField: uuid.NewString(),
		BoolField:   true,
		BytesField:  []byte(uuid.NewString()),
		NewField:    uuid.NewString(),
	}
	_, err = writer2.Write(ctx, &evt2)
	require.NoError(t, err)

	consumerTopicConfig := zkafka.ConsumerTopicConfig{
		ClientID:  fmt.Sprintf("reader-%s-%s", t.Name(), uuid.NewString()),
		Topic:     topic,
		Formatter: zkafka.JSONSchemaRegistry,
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

	receivedEvt1 := json1.DummyEvent{}
	require.NoError(t, msg1.Decode(&receivedEvt1))
	assertEqual(t, evt1, receivedEvt1, cmpopts.IgnoreUnexported(json1.DummyEvent{}))

	receivedEvt2Schema1 := json1.DummyEvent{}
	require.NoError(t, msg2.Decode(&receivedEvt2Schema1))
	expectedEvt2 := json1.DummyEvent{
		IntField:    evt2.IntField,
		DoubleField: evt2.DoubleField,
		StringField: evt2.StringField,
		BoolField:   evt2.BoolField,
		BytesField:  evt2.BytesField,
	}
	assertEqual(t, expectedEvt2, receivedEvt2Schema1, cmpopts.IgnoreUnexported(json1.DummyEvent{}))

	receivedEvt2Schema2 := json2.DummyEvent{}
	require.NoError(t, msg2.Decode(&receivedEvt2Schema2))
	assertEqual(t, evt2, receivedEvt2Schema2, cmpopts.IgnoreUnexported(json2.DummyEvent{}))
}
