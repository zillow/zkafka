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

type schemaRegistryFactory struct {
	mmu   sync.Mutex
	srCls map[string]schemaregistry.Client
}

func newSchemaRegistryFactory() *schemaRegistryFactory {
	return &schemaRegistryFactory{
		srCls: make(map[string]schemaregistry.Client),
	}
}

func (c *schemaRegistryFactory) createAvro(srConfig SchemaRegistryConfig) (avroFmt, error) {
	cl, err := c.getSchemaClient(srConfig)
	if err != nil {
		return avroFmt{}, err
	}

	deserConfig := avrov2.NewDeserializerConfig()
	deser, err := avrov2.NewDeserializer(cl, serde.ValueSerde, deserConfig)
	if err != nil {
		return avroFmt{}, fmt.Errorf("failed to create deserializer: %w", err)
	}

	serConfig := avrov2.NewSerializerConfig()
	serConfig.AutoRegisterSchemas = srConfig.Serialization.AutoRegisterSchemas
	serConfig.NormalizeSchemas = true

	ser, err := avrov2.NewSerializer(cl, serde.ValueSerde, serConfig)
	if err != nil {
		return avroFmt{}, fmt.Errorf("failed to create serializer: %w", err)
	}
	return avroFmt{
		ser:   ser,
		deser: deser,
	}, nil
}

func (c *schemaRegistryFactory) createProto(srConfig SchemaRegistryConfig) (protoFmt, error) {
	cl, err := c.getSchemaClient(srConfig)
	if err != nil {
		return protoFmt{}, err
	}

	deserConfig := protobuf.NewDeserializerConfig()
	deser, err := protobuf.NewDeserializer(cl, serde.ValueSerde, deserConfig)
	if err != nil {
		return protoFmt{}, fmt.Errorf("failed to create deserializer: %w", err)
	}

	serConfig := protobuf.NewSerializerConfig()
	serConfig.AutoRegisterSchemas = srConfig.Serialization.AutoRegisterSchemas
	serConfig.NormalizeSchemas = true

	ser, err := protobuf.NewSerializer(cl, serde.ValueSerde, serConfig)
	if err != nil {
		return protoFmt{}, fmt.Errorf("failed to create serializer: %w", err)
	}
	return protoFmt{
		ser:   ser,
		deser: deser,
	}, nil

}

func (c *schemaRegistryFactory) createJson(srConfig SchemaRegistryConfig) (jsonFmt, error) {
	cl, err := c.getSchemaClient(srConfig)
	if err != nil {
		return jsonFmt{}, err
	}

	deserConfig := jsonschema.NewDeserializerConfig()
	deser, err := jsonschema.NewDeserializer(cl, serde.ValueSerde, deserConfig)
	if err != nil {
		return jsonFmt{}, fmt.Errorf("failed to create deserializer: %w", err)
	}

	serConfig := jsonschema.NewSerializerConfig()
	serConfig.AutoRegisterSchemas = srConfig.Serialization.AutoRegisterSchemas
	serConfig.NormalizeSchemas = true

	ser, err := jsonschema.NewSerializer(cl, serde.ValueSerde, serConfig)
	if err != nil {
		return jsonFmt{}, fmt.Errorf("failed to create serializer: %w", err)
	}
	return jsonFmt{
		ser:   ser,
		deser: deser,
	}, nil

}

func (c *schemaRegistryFactory) getSchemaClient(srConfig SchemaRegistryConfig) (schemaregistry.Client, error) {
	url := srConfig.URL
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

type avroFmt struct {
	ser   *avrov2.Serializer
	deser *avrov2.Deserializer
}

func (s avroFmt) GetID(topic string, avroSchema string) (int, error) {
	return s.ser.GetID(topic, nil, &schemaregistry.SchemaInfo{Schema: avroSchema})
}

func (s avroFmt) Deserialize(topic string, value []byte, target any) error {
	return s.deser.DeserializeInto(topic, value, target)
}

type protoFmt struct {
	ser   *protobuf.Serializer
	deser *protobuf.Deserializer
}

type jsonFmt struct {
	ser   *jsonschema.Serializer
	deser *jsonschema.Deserializer
}
