package zkafka

import (
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"
	"github.com/zillow/zfmt"
)

const (
	// CustomFmt indicates that the user would pass in their own Formatter later
	CustomFmt        zfmt.FormatterType = "custom"
	AvroConfluentFmt zfmt.FormatterType = "avro_confluent"
)

var errMissingFmtter = errors.New("custom formatter is missing, did you forget to call WithFormatter()")

// Formatter allows the user to extend formatting capability to unsupported data types
type Formatter interface {
	Marshall(v any) ([]byte, error)
	Unmarshal(b []byte, v any) error
}

type confluentFormatter interface {
	Marshall(topic string, v any, schema string) ([]byte, error)
	Unmarshal(topic string, b []byte, v any) error
}

func getFormatter(formatter zfmt.FormatterType, schemaID int, srCfg SchemaRegistryConfig) (Formatter, confluentFormatter, error) {
	switch formatter {
	case AvroConfluentFmt:
		cf, err := newAvroSchemaRegistryFormatter(srCfg)
		return nil, cf, err
	case CustomFmt:
		return &errFormatter{}, nil, nil
	default:
		f, err := zfmt.GetFormatter(formatter, schemaID)
		if err != nil {
			return nil, nil, fmt.Errorf("unsupported formatter %s", formatter)
		}
		return f, nil, nil
	}
}

// errFormatter is a formatter that returns error when called. The error will remind the user
// to provide appropriate implementation
type errFormatter struct{}

// Marshall returns error with reminder
func (f errFormatter) Marshall(_ any) ([]byte, error) {
	return nil, errMissingFmtter
}

// Unmarshal returns error with reminder
func (f errFormatter) Unmarshal(_ []byte, _ any) error {
	return errMissingFmtter
}

var _ confluentFormatter = (*avroSchemaRegistryFormatter)(nil)

type avroSchemaRegistryFormatter struct {
	deser *avrov2.Deserializer
	ser   *avrov2.Serializer
	f     zfmt.SchematizedAvroFormatter
}

func newAvroSchemaRegistryFormatter(srConfig SchemaRegistryConfig) (avroSchemaRegistryFormatter, error) {
	url := srConfig.URL
	if url == "" {
		return avroSchemaRegistryFormatter{}, errors.New("no schema registry url provided")

	}
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(url))
	if err != nil {
		return avroSchemaRegistryFormatter{}, fmt.Errorf("failed to create schema registry client: %w", err)
	}

	deserConfig := avrov2.NewDeserializerConfig()
	deser, err := avrov2.NewDeserializer(client, serde.ValueSerde, deserConfig)
	if err != nil {
		return avroSchemaRegistryFormatter{}, fmt.Errorf("failed to create deserializer: %w", err)
	}

	serConfig := avrov2.NewSerializerConfig()
	serConfig.AutoRegisterSchemas = srConfig.Serialization.AutoRegisterSchemas
	serConfig.NormalizeSchemas = true

	ser, err := avrov2.NewSerializer(client, serde.ValueSerde, serConfig)
	if err != nil {
		return avroSchemaRegistryFormatter{}, fmt.Errorf("failed to create serializer: %w", err)
	}
	return avroSchemaRegistryFormatter{
		deser: deser,
		ser:   ser,
	}, nil
}

func (f avroSchemaRegistryFormatter) Marshall(topic string, target any, avroSchema string) ([]byte, error) {
	if avroSchema != "" {
		info := schemaregistry.SchemaInfo{
			Schema: avroSchema,
		}
		id, err := f.ser.GetID(topic, nil, &info)
		if err != nil {
			return nil, fmt.Errorf("failed to get avro schema by id: %w", err)
		}
		f.f.SchemaID = id
		return f.f.Marshall(target)
	}
	value, err := f.ser.Serialize(topic, target)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize confluent schema registry avro type: %w", err)
	}
	return value, nil
}

func (f avroSchemaRegistryFormatter) Unmarshal(topic string, value []byte, target any) error {
	err := f.deser.DeserializeInto(topic, value, &target)
	if err != nil {
		return fmt.Errorf("failed to deserialize to confluent schema registry avro type: %w", err)
	}
	return nil
}
