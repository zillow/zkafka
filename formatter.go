package zkafka

import (
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
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

type marshReq struct {
	topic string
	// subject is the data to be marshalled
	subject any
	// schema
	schema string
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

type formatterArgs struct {
	formatter zfmt.FormatterType
	schemaID  int
	srCfg     SchemaRegistryConfig
	getSR     srProvider2
}

func getFormatter(args formatterArgs) (kFormatter, error) {
	formatter := args.formatter
	schemaID := args.schemaID

	switch formatter {
	case AvroConfluentFmt:
		srCfg := args.srCfg
		getSR := args.getSR
		scl, err := getSR(srCfg)
		if err != nil {
			return nil, err
		}
		cf, err := NewAvroSchemaRegistryFormatter(scl)
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

type avroSchemaRegistryFormatter struct {
	schemaRegistryCl schemaRegistryCl
	f                zfmt.SchematizedAvroFormatter
}

func NewAvroSchemaRegistryFormatter(shimCl schemaRegistryCl) (avroSchemaRegistryFormatter, error) {
	return avroSchemaRegistryFormatter{
		schemaRegistryCl: shimCl,
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
	err := f.schemaRegistryCl.Deserialize(req.topic, req.data, &req.target)
	if err != nil {
		return fmt.Errorf("failed to deserialize to confluent schema registry avro type: %w", err)
	}
	return nil
}

type schemaRegistryCl interface {
	GetID(topic string, avroSchema string) (int, error)
	Deserialize(topic string, value []byte, target any) error
}

var _ schemaRegistryCl = (*shim)(nil)

type shim struct {
	ser   *avrov2.Serializer
	deser *avrov2.Deserializer
}

func (s shim) GetID(topic string, avroSchema string) (int, error) {
	return s.ser.GetID(topic, nil, &schemaregistry.SchemaInfo{Schema: avroSchema})
}

func (s shim) Deserialize(topic string, value []byte, target any) error {
	return s.deser.DeserializeInto(topic, value, target)
}
