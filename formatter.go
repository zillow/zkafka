package zkafka

import (
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	"github.com/zillow/zfmt"
	//"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
)

const (
	// CustomFmt indicates that the user would pass in their own Formatter later
	CustomFmt zfmt.FormatterType = "custom"
	// AvroSchemaRegistry uses confluent's schema registry. It encodes a schemaID as the first 5 bytes and then avro serializes (binary)
	// for the remaining part of the payload. It is the successor to `avro_schema` which ships with zfmt,
	AvroSchemaRegistry  zfmt.FormatterType = "avro_schema_registry"
	ProtoSchemaRegistry zfmt.FormatterType = "proto_schema_registry"
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
}

// errFormatter is a formatter that returns error when called. The error will remind the user
// to provide appropriate implementation
type errFormatter struct{}

// marshall returns error with reminder
func (f errFormatter) marshall(req marshReq) ([]byte, error) {
	return nil, errMissingFmtter
}

// unmarshal returns error with reminder
func (f errFormatter) unmarshal(req unmarshReq) error {
	return errMissingFmtter
}

type avroSchemaRegistryFormatter struct {
	afmt avroFmt
	f    zfmt.SchematizedAvroFormatter
}

func newAvroSchemaRegistryFormatter(afmt avroFmt) (avroSchemaRegistryFormatter, error) {
	return avroSchemaRegistryFormatter{
		afmt: afmt,
	}, nil
}

func (f avroSchemaRegistryFormatter) marshall(req marshReq) ([]byte, error) {
	if req.schema == "" {
		return nil, errors.New("avro schema is required for schema registry formatter")
	}
	id, err := f.afmt.GetID(req.topic, req.schema)
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
	err := f.afmt.Deserialize(req.topic, req.data, &req.target)
	if err != nil {
		return fmt.Errorf("failed to deserialize to confluent schema registry avro type: %w", err)
	}
	return nil
}

type protoSchemaRegistryFormatter struct {
	pfmt protoFmt
}

func newProtoSchemaRegistryFormatter(pfmt protoFmt) protoSchemaRegistryFormatter {
	return protoSchemaRegistryFormatter{
		pfmt: pfmt,
	}
}

func (f protoSchemaRegistryFormatter) marshall(req marshReq) ([]byte, error) {
	msgBytes, err := f.pfmt.ser.Serialize(req.topic, req.subject)
	if err != nil {
		return nil, fmt.Errorf("failed to proto serialize: %w", err)
	}
	return msgBytes, nil
}

func (f protoSchemaRegistryFormatter) unmarshal(req unmarshReq) error {
	if err := f.pfmt.deser.DeserializeInto(req.topic, req.data, req.target); err != nil {
		return fmt.Errorf("failed to proto deserialize: %w", err)
	}
	return nil
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
