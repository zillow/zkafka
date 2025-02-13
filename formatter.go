package zkafka

import (
	"errors"
	"fmt"

	"github.com/hamba/avro/v2"
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
	// v is the data to be marshalled
	v any
	// schema is currently only used for avro schematizations. It is necessary,
	// because the confluent implementation reflects on the v to get the schema to use for
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
	// schema is currently only used for avro schematizations. It is necessary,
	// because the confluent implementation reflects on the subject to get the schema to use for
	// communicating with schema-registry and backward compatible evolutions fail beause if dataloss during reflection.
	// For example, if a field has a default value, the reflection doesn't pick this up
	schema string
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
	return f.F.Marshall(req.v)
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
}

func newAvroSchemaRegistryFormatter(afmt avroFmt) (avroSchemaRegistryFormatter, error) {
	return avroSchemaRegistryFormatter{
		afmt: afmt,
	}, nil
}

// marshall looks a subject's schema (id) so that it can prefix the eventual message payload.
// A schema must be provided and hamba/avro is used in conjunction with this schema to marshall they payload.
// Structs generated using hamba/avro work best, since they provide avro tags which handles casing
// which can lead to errors otherwise.
func (f avroSchemaRegistryFormatter) marshall(req marshReq) ([]byte, error) {
	if req.schema == "" {
		return nil, errors.New("avro schema is required for schema registry formatter")
	}
	id, err := f.afmt.GetID(req.topic, req.schema)
	if err != nil {
		return nil, fmt.Errorf("failed to get avro schema by id for topic %s: %w", req.topic, err)
	}
	avroSchema, err := avro.Parse(req.schema)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema from payload: %w", err)
	}
	msgBytes, err := avro.Marshal(avroSchema, req.v)
	if err != nil {
		return nil, fmt.Errorf("failed to marhall payload per avro schema: %w", err)
	}
	payload, err := f.afmt.ser.WriteBytes(id, msgBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to prepend schema related bytes: %w", err)
	}
	return payload, nil
}

// unmarshall looks up the schema based on the schemaID in the message payload (`dataSchema`).
// Additionally, a target schema is provided in the request (`targetSchema`).
//
// The 'targetSchema' and 'dataSchema' are resolved so that data written by the `dataSchema` may
// be read by the `targetSchema`.
//
// The avro.Deserializer has much of this functionality built in, other than being able to specify
// the `targetSchema` and instead infers the target schema from the target struct. This creates
// issues in some common use cases.
func (f avroSchemaRegistryFormatter) unmarshal(req unmarshReq) error {
	if req.schema == "" {
		return errors.New("avro schema is required for schema registry formatter")
	}
	inInfo, err := f.afmt.deser.GetSchema(req.topic, req.data)
	if err != nil {
		return fmt.Errorf("failed to get schema from message payload: %w", err)
	}

	// schema of data that exists on the wire, that is about to be marshalled into the schema of our target
	dataSchema, err := avro.Parse(inInfo.Schema)
	if err != nil {
		return fmt.Errorf("failed to parse schema associated with message: %w", err)
	}

	targetSchema, err := avro.Parse(req.schema)
	if err != nil {
		return fmt.Errorf("failed to parse schema : %w", err)
	}

	sc := avro.NewSchemaCompatibility()
	if err := sc.Compatible(dataSchema, targetSchema); err != nil {
		return fmt.Errorf("producer schema incompatible with consumer schema: %w", err)
	}

	err = avro.Unmarshal(targetSchema, req.data[5:], req.target)
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
	msgBytes, err := f.pfmt.ser.Serialize(req.topic, req.v)
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
	msgBytes, err := f.jfmt.ser.Serialize(req.topic, req.v)
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
