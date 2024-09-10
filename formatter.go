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

func getFormatter2(formatter zfmt.FormatterType, srCfg SchemaRegistryConfig, getSR srProvider) (confluentFormatter, error) {
	switch formatter {
	case AvroConfluentFmt:
		cl, err := getSR(srCfg)
		if err != nil {
			return nil, err
		}
		cf, err := newAvroSchemaRegistryFormatter(cl, srCfg)
		return cf, err
	default:
		return nil, fmt.Errorf("unsupported formatter %s", formatter)
	}
}

func getFormatter(formatter zfmt.FormatterType, schemaID int) (Formatter, error) {
	switch formatter {
	case CustomFmt:
		return &errFormatter{}, nil
	default:
		f, err := zfmt.GetFormatter(formatter, schemaID)
		if err != nil {
			return nil, fmt.Errorf("unsupported formatter %s", formatter)
		}
		return f, nil
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
	schemaRegistryCl schemaRegistryCl
	//deser *avrov2.Deserializer
	//ser   *avrov2.Serializer
	f zfmt.SchematizedAvroFormatter
}

//	func newAvroConfig(srConfig SchemaRegistryConfig) (*avro.SerializerConfig, *avro.DeserializerConfig, error) {
//		url := srConfig.URL
//		if url == "" {
//			return nil, nil, errors.New("no schema registry url provided")
//		}
//		deserConfig := avrov2.NewDeserializerConfig()
//
//		serConfig := avrov2.NewSerializerConfig()
//		serConfig.AutoRegisterSchemas = srConfig.Serialization.AutoRegisterSchemas
//		serConfig.NormalizeSchemas = true
//
// }
func newAvroSchemaRegistryFormatter(cl schemaregistry.Client, srConfig SchemaRegistryConfig) (avroSchemaRegistryFormatter, error) {
	url := srConfig.URL
	if url == "" {
		return avroSchemaRegistryFormatter{}, errors.New("no schema registry url provided")
	}

	deserConfig := avrov2.NewDeserializerConfig()
	deser, err := avrov2.NewDeserializer(cl, serde.ValueSerde, deserConfig)
	if err != nil {
		return avroSchemaRegistryFormatter{}, fmt.Errorf("failed to create deserializer: %w", err)
	}

	serConfig := avrov2.NewSerializerConfig()
	serConfig.AutoRegisterSchemas = srConfig.Serialization.AutoRegisterSchemas
	serConfig.NormalizeSchemas = true

	ser, err := avrov2.NewSerializer(cl, serde.ValueSerde, serConfig)
	if err != nil {
		return avroSchemaRegistryFormatter{}, fmt.Errorf("failed to create serializer: %w", err)
	}
	shimcl := shim{
		ser:   ser,
		deser: deser,
	}
	return avroSchemaRegistryFormatter{
		schemaRegistryCl: shimcl,
	}, nil
}

func (f avroSchemaRegistryFormatter) Marshall(topic string, target any, avroSchema string) ([]byte, error) {
	if avroSchema == "" {
		return nil, errors.New("avro schema is required for schema registry formatter")
	}
	id, err := f.schemaRegistryCl.GetID(topic, avroSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to get avro schema by id for topic %s: %w", topic, err)
	}
	f.f.SchemaID = id
	data, err := f.f.Marshall(target)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal avro schema for topic %s: %w", topic, err)
	}
	return data, nil
}

func (f avroSchemaRegistryFormatter) Unmarshal(topic string, value []byte, target any) error {
	err := f.schemaRegistryCl.DeserializeInto(topic, value, &target)
	if err != nil {
		return fmt.Errorf("failed to deserialize to confluent schema registry avro type: %w", err)
	}
	return nil
}

type schemaRegistryCl interface {
	GetID(topic string, avroSchema string) (int, error)
	DeserializeInto(topic string, value []byte, target any) error
}

var _ schemaRegistryCl = (*shim)(nil)

type shim struct {
	ser   *avrov2.Serializer
	deser *avrov2.Deserializer
}

func (s shim) GetID(topic string, avroSchema string) (int, error) {
	return s.ser.GetID(topic, nil, &schemaregistry.SchemaInfo{Schema: avroSchema})
}

func (s shim) DeserializeInto(topic string, value []byte, target any) error {
	return s.deser.DeserializeInto(topic, value, target)
}
