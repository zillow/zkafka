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

//type confluentFormatter interface {
//	marshall(topic string, v any, schema string) ([]byte, error)
//	unmarshal(topic string, b []byte, v any) error
//}

type marshReq struct {
	topic   string
	subject any
	schema  string
}

type unmarshReq struct {
	topic  string
	data   []byte
	target any
}

type kFormatter interface {
	marshall(req marshReq) ([]byte, error)
	unmarshal(req unmarshReq) error
}

var _ kFormatter = (*avroSchemaRegistryFormatter)(nil)
var _ kFormatter = (*zfmtShim)(nil)

type zfmtShim struct {
	F zfmt.Formatter
}

func (f zfmtShim) marshall(req marshReq) ([]byte, error) {
	return f.F.Marshall(req.subject)
}

func (f zfmtShim) unmarshal(req unmarshReq) error {
	return f.F.Unmarshal(req.data, req.target)
}

func getFormatter(formatter zfmt.FormatterType, schemaID int, srCfg SchemaRegistryConfig, getSR srProvider) (kFormatter, error) {
	switch formatter {
	case AvroConfluentFmt:
		cl, err := getSR(srCfg)
		if err != nil {
			return nil, err
		}
		cf, err := newAvroSchemaRegistryFormatter(cl, srCfg)
		return cf, err
	case CustomFmt:
		return &errFormatter{}, nil
	default:
		f, err := zfmt.GetFormatter(formatter, schemaID)
		if err != nil {
			return nil, fmt.Errorf("unsupported formatter %s", formatter)
		}
		return zfmtShim{F: f}, nil
	}
}

// errFormatter is a formatter that returns error when called. The error will remind the user
// to provide appropriate implementation
type errFormatter struct{}

// Marshall returns error with reminder
func (f errFormatter) marshall(req marshReq) ([]byte, error) {
	return nil, errMissingFmtter
}

// Unmarshal returns error with reminder
func (f errFormatter) unmarshal(req unmarshReq) error {
	return errMissingFmtter
}

//var _ confluentFormatter = (*avroSchemaRegistryFormatter)(nil)

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

func (f avroSchemaRegistryFormatter) marshall(req marshReq) ([]byte, error) {
	if req.schema == "" {
		return nil, errors.New("avro schema is required for schema registry formatter")
	}
	id, err := f.schemaRegistryCl.GetID(req.topic, req.schema)
	if err != nil {
		return nil, fmt.Errorf("failed to get avro schema by id for topic %s: %w", req.topic, err)
	}
	f.f.SchemaID = id
	data, err := f.f.Marshall(req.subject)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal avro schema for topic %s: %w", req.topic, err)
	}
	return data, nil
}

func (f avroSchemaRegistryFormatter) unmarshal(req unmarshReq) error {
	err := f.schemaRegistryCl.DeserializeInto(req.topic, req.data, &req.target)
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
