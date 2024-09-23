package zkafka

import (
	"errors"
	"fmt"

	"github.com/zillow/zfmt"
)

const (
	// CustomFmt indicates that the user would pass in their own Formatter later
	CustomFmt zfmt.FormatterType = "custom"
	// AvroSchemaRegistry uses confluent's schema registry. It encodes a schemaID as the first 5 bytes and then avro serializes (binary)
	// for the remaining part of the payload. It is the successor to `avro_schema` which ships with zfmt,
	AvroSchemaRegistry zfmt.FormatterType = "avro_schema_registry"

	// ProtoSchemaRegistry uses confluent's schema registry. It encodes a schemaID as well as the message types as
	// a payload prefix and then proto serializes (binary) for the remaining part of the payload.
	// zfmt.ProtoSchemaDeprecatedFmt had a bug in its implementation and didn't work properly with confluent
	ProtoSchemaRegistry zfmt.FormatterType = "proto_schema_registry"

	// JSONSchemaRegistry uses confluent's schema registry. It encodes a schemaID as the first 5 bytes and then json serializes (human readable)
	// for the remaining part of the payload. It is the successor to `json_schema` which ships with zfmt,
	JSONSchemaRegistry zfmt.FormatterType = "json_schema_registry"
)

var errMissingFormatter = errors.New("custom formatter is missing, did you forget to call WithFormatter()")

// Formatter allows the user to extend formatting capability to unsupported data types
type Formatter interface {
	Marshall(v any) ([]byte, error)
	Unmarshal(b []byte, v any) error
}

type marshReq struct {
	// topic is the kafka topic being written to
	topic string
	// subject is the data to be marshalled
	subject any
	// schema is currently only used for avro schematizations. It is necessary,
	// because the confluent implementation reflects on the subject to get the schema to use for
	// communicating with schema-registry and backward compatible evolutions fail beause if dataloss during reflection.
	// For example, if a field has a default value, the reflection doesn't pick this up
	schema string
}

type unmarshReq struct {
	// topic is the kafka topic being read from
	topic string
	// data is the message value which will be unmarshalled to a type
	data []byte
	// target is the stuct which is to be hydrated by the contents of data
	target any
}

var _ kFormatter = (*avroSchemaRegistryFormatter)(nil)
var _ kFormatter = (*zfmtShim)(nil)

// kFormatter is zkafka special formatter.
// It extends zfmt options, and works with schema registry.
type kFormatter interface {
	marshall(req marshReq) ([]byte, error)
	unmarshal(req unmarshReq) error
}

// zfmtShim is a shim type which allows
// zfmt formatters to work the kFormatter
type zfmtShim struct {
	F zfmt.Formatter
}

func (f zfmtShim) marshall(req marshReq) ([]byte, error) {
	return f.F.Marshall(req.subject)
}

func (f zfmtShim) unmarshal(req unmarshReq) error {
	return f.F.Unmarshal(req.data, req.target)
}

// errFormatter is a formatter that returns error when called. The error will remind the user
// to provide appropriate implementation
type errFormatter struct{}

// marshall returns error with reminder
func (f errFormatter) marshall(_ marshReq) ([]byte, error) {
	return nil, errMissingFormatter
}

// unmarshal returns error with reminder
func (f errFormatter) unmarshal(_ unmarshReq) error {
	return errMissingFormatter
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
	err := f.afmt.Deserialize(req.topic, req.data, req.target)
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

type jsonSchemaRegistryFormatter struct {
	jfmt jsonFmt
}

func newJsonSchemaRegistryFormatter(jfmt jsonFmt) jsonSchemaRegistryFormatter {
	return jsonSchemaRegistryFormatter{
		jfmt: jfmt,
	}
}

func (f jsonSchemaRegistryFormatter) marshall(req marshReq) ([]byte, error) {
	msgBytes, err := f.jfmt.ser.Serialize(req.topic, req.subject)
	if err != nil {
		return nil, fmt.Errorf("failed to json schema serialize: %w", err)
	}
	return msgBytes, nil
}

func (f jsonSchemaRegistryFormatter) unmarshal(req unmarshReq) error {
	if err := f.jfmt.deser.DeserializeInto(req.topic, req.data, req.target); err != nil {
		return fmt.Errorf("failed to json schema deserialize: %w", err)
	}
	return nil
}
