package zkafka

import (
	"errors"
	"fmt"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
)

type SchemaRegistryConfig struct {
	// URL is the schema registry URL. During serialization and deserialization
	// schema registry is checked against to confirm schema compatability.
	URL string
	// Serialization provides additional information used by schema registry marshalers during serialization (data write)
	Serialization SerializationConfig
	// Deserialization provides additional information used by schema registry marshalers during deserialization (data read)
	Deserialization DeserializationConfig
	// SubjectName allows the specification of the SubjectName. If not specified defaults to [topic name strategy](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#subject-name-strategy)
	SubjectName string
}

type SerializationConfig struct {
	// AutoRegisterSchemas indicates whether new schemas (those that evolve existing schemas or are brand new) should be registered
	// with schema registry dynamically. This feature is typically not used for production workloads
	AutoRegisterSchemas bool
	// Schema is used exclusively by the avro schema registry marshaler today. Its necessary to provide proper schema evolution properties
	// expected by typical use cases.
	Schema string
}

type DeserializationConfig struct {
	// Schema is used exclusively by the avro schema registry marshaler today. It's necessary to provide proper schema evolution properties
	// expected by typical use cases.
	Schema string
}

func subjectNameStrategy(cfg SchemaRegistryConfig) serde.SubjectNameStrategyFunc {
	return func(topic string, serdeType serde.Type, schema schemaregistry.SchemaInfo) (string, error) {
		if cfg.SubjectName != "" {
			return cfg.SubjectName, nil
		}
		return serde.TopicNameStrategy(topic, serdeType, schema)
	}
}

type SchemaRegistryFactory struct {
	m     sync.Mutex
	srCls map[string]schemaregistry.Client
	cfg   SchemaRegistryConfig
}

func newSchemaRegistryFactory() *SchemaRegistryFactory {
	return &SchemaRegistryFactory{
		srCls: make(map[string]schemaregistry.Client),
	}
}

func (c *SchemaRegistryFactory) getSchemaClient(rConfig SchemaRegistryConfig) (schemaregistry.Client, error) {
	c.m.Lock()
	defer c.m.Unlock()

	url := rConfig.URL
	if url == "" {
		return nil, errors.New("no schema registry url provided")
	}
	if srCl, ok := c.srCls[url]; ok {
		return srCl, nil
	}
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(url))
	if err != nil {
		return nil, fmt.Errorf("failed to create schema registry client: %w", err)
	}
	c.srCls[url] = client
	return client, nil
}

func (c *SchemaRegistryFactory) createAvro(rConfig SchemaRegistryConfig) (avroMarshaler, error) {
	cl, err := c.getSchemaClient(rConfig)
	if err != nil {
		return avroMarshaler{}, err
	}

	deserConfig := avrov2.NewDeserializerConfig()
	deser, err := avrov2.NewDeserializer(cl, serde.ValueSerde, deserConfig)
	if err != nil {
		return avroMarshaler{}, fmt.Errorf("failed to create deserializer: %w", err)
	}
	deser.SubjectNameStrategy = subjectNameStrategy(rConfig)

	serConfig := avrov2.NewSerializerConfig()
	serConfig.AutoRegisterSchemas = rConfig.Serialization.AutoRegisterSchemas
	serConfig.NormalizeSchemas = true

	ser, err := avrov2.NewSerializer(cl, serde.ValueSerde, serConfig)
	if err != nil {
		return avroMarshaler{}, fmt.Errorf("failed to create serializer: %w", err)
	}
	ser.SubjectNameStrategy = subjectNameStrategy(rConfig)

	return avroMarshaler{
		ser:   ser,
		deser: deser,
	}, nil
}

func (c *SchemaRegistryFactory) createProto(rConfig SchemaRegistryConfig) (protoMarshaler, error) {
	cl, err := c.getSchemaClient(rConfig)
	if err != nil {
		return protoMarshaler{}, err
	}

	deserConfig := protobuf.NewDeserializerConfig()
	deser, err := protobuf.NewDeserializer(cl, serde.ValueSerde, deserConfig)
	if err != nil {
		return protoMarshaler{}, fmt.Errorf("failed to create deserializer: %w", err)
	}

	deser.SubjectNameStrategy = subjectNameStrategy(rConfig)

	serConfig := protobuf.NewSerializerConfig()
	serConfig.AutoRegisterSchemas = rConfig.Serialization.AutoRegisterSchemas
	serConfig.NormalizeSchemas = true

	ser, err := protobuf.NewSerializer(cl, serde.ValueSerde, serConfig)
	if err != nil {
		return protoMarshaler{}, fmt.Errorf("failed to create serializer: %w", err)
	}
	ser.SubjectNameStrategy = subjectNameStrategy(rConfig)
	return protoMarshaler{
		ser:   ser,
		deser: deser,
	}, nil
}

func (c *SchemaRegistryFactory) createJson(srConfig SchemaRegistryConfig) (jsonMarshaler, error) {
	cl, err := c.getSchemaClient(srConfig)
	if err != nil {
		return jsonMarshaler{}, err
	}

	deserConfig := jsonschema.NewDeserializerConfig()
	deser, err := jsonschema.NewDeserializer(cl, serde.ValueSerde, deserConfig)
	if err != nil {
		return jsonMarshaler{}, fmt.Errorf("failed to create deserializer: %w", err)
	}
	deser.SubjectNameStrategy = subjectNameStrategy(srConfig)

	serConfig := jsonschema.NewSerializerConfig()
	serConfig.AutoRegisterSchemas = srConfig.Serialization.AutoRegisterSchemas
	serConfig.NormalizeSchemas = true

	ser, err := jsonschema.NewSerializer(cl, serde.ValueSerde, serConfig)
	if err != nil {
		return jsonMarshaler{}, fmt.Errorf("failed to create serializer: %w", err)
	}
	ser.SubjectNameStrategy = subjectNameStrategy(srConfig)

	return jsonMarshaler{
		ser:   ser,
		deser: deser,
	}, nil
}
