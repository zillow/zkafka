package zkafka

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	"github.com/hamba/avro/v2"
)

type MarshReq struct {
	// topic is the kafka topic being written to
	topic string
	// v is the data to be marshaled
	v any
}

type UnmarshReq struct {
	// topic is the kafka topic being read from
	topic string
	// data is the message value which will be unmarshaled to a type
	data []byte
	// target is the stuct which is to be hydrated by the contents of data
	target any
}

var _ KMarshaler = (*AvroSchemaRegistryMarshaler)(nil)
var _ KMarshaler = (*ProtoSchemaRegistryMarshaler)(nil)
var _ KMarshaler = (*JsonSchemaRegistryMarshaler)(nil)

// KMarshaler is a zkafka special marshaler.
type KMarshaler interface {
	Marshal(req MarshReq) ([]byte, error)
	Unmarshal(req UnmarshReq) error
}

type KMarshalerFactory interface {
	GetMarshaler() (KMarshaler, error)
}

// Marshaler is a generic marshaler interface
type Marshaler interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(b []byte, v any) error
}

// JSONMarshaler is a Marshaler backed by encoding/json. It is the default
// marshaler used when a topic config does not specify a MarshalerFactory.
type JSONMarshaler struct{}

// Marshal encodes v as json
func (JSONMarshaler) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal decodes json into v
func (JSONMarshaler) Unmarshal(b []byte, v any) error {
	return json.Unmarshal(b, v)
}

// KMarshalerShim adapts a plain Marshaler (such as a zfmt formatter) to the
// zkafka interfaces. It is both a KMarshaler, so it can be passed to
// RMarshalerOption / WMarshalerOption, and a KMarshalerFactory that returns
// itself, so it can be set as a topic config's MarshalerFactory.
type KMarshalerShim struct {
	F Marshaler
}

var _ KMarshaler = KMarshalerShim{}
var _ KMarshalerFactory = KMarshalerShim{}

func (f KMarshalerShim) Marshal(req MarshReq) ([]byte, error) {
	return f.F.Marshal(req.v)
}

func (f KMarshalerShim) Unmarshal(req UnmarshReq) error {
	return f.F.Unmarshal(req.data, req.target)
}

// GetMarshaler returns the shim itself; a plain Marshaler needs no per-reader setup.
func (f KMarshalerShim) GetMarshaler() (KMarshaler, error) {
	return f, nil
}

type avroMarshaler struct {
	ser   *avrov2.Serializer
	deser *avrov2.Deserializer
}

func (s avroMarshaler) GetID(topic string, avroSchema string) (int, error) {
	return s.ser.GetID(topic, nil, &schemaregistry.SchemaInfo{Schema: avroSchema})
}

type protoMarshaler struct {
	ser   *protobuf.Serializer
	deser *protobuf.Deserializer
}

type jsonMarshaler struct {
	ser   *jsonschema.Serializer
	deser *jsonschema.Deserializer
}

type AvroSchemaRegistryMarshaler struct {
	// serSchema is the schema values are marshaled with (SchemaRegistryConfig.Serialization.Schema)
	serSchema string
	// deserSchema is the target schema values are unmarshaled into (SchemaRegistryConfig.Deserialization.Schema)
	deserSchema string
	aMarshaler  avroMarshaler
	// resolvedSchemas caches the resolved (target<-data) avro schema keyed by the
	// message's 4-byte confluent schema ID. Schema compatibility resolution
	// fingerprints the full schema (Schema.String over the whole tree), which is
	// very expensive for large schemas; without caching it runs on every message.
	// The target schema is fixed for a given marshaler, so the schema ID alone is
	// a sufficient key. Held by pointer so the value-copied marshaler shares it.
	resolvedSchemas *sync.Map
}

func newAvroSchemaRegistryMarshaler(
	aMarshaler avroMarshaler,
	serSchema string,
	deserSchema string,
) AvroSchemaRegistryMarshaler {
	return AvroSchemaRegistryMarshaler{
		serSchema:       serSchema,
		deserSchema:     deserSchema,
		aMarshaler:      aMarshaler,
		resolvedSchemas: &sync.Map{},
	}
}

// Marshal looks a subject's schema (id) so that it can prefix the eventual message payload.
// A schema must be provided and hamba/avro is used in conjunction with this schema to marshal they payload.
// Structs generated using hamba/avro work best, since they provide avro tags which handles casing
// which can lead to errors otherwise.
func (f AvroSchemaRegistryMarshaler) Marshal(req MarshReq) ([]byte, error) {
	if f.serSchema == "" {
		return nil, errors.New("avro schema is required for schema registry marshaler: set SchemaRegistryConfig.Serialization.Schema")
	}
	id, err := f.aMarshaler.GetID(req.topic, f.serSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to get avro schema by id for topic %s: %w", req.topic, err)
	}
	avroSchema, err := avro.Parse(f.serSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema from payload: %w", err)
	}
	msgBytes, err := avro.Marshal(avroSchema, req.v)
	if err != nil {
		return nil, fmt.Errorf("failed to marhall payload per avro schema: %w", err)
	}
	payload, err := f.aMarshaler.ser.WriteBytes(id, msgBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to prepend schema related bytes: %w", err)
	}
	return payload, nil
}

// Unmarshal looks up the schema based on the schemaID in the message payload (`dataSchema`).
// Additionally, a target schema is provided in the request (`targetSchema`).
//
// The 'targetSchema' and 'dataSchema' are resolved so that data written by the `dataSchema` may
// be read by the `targetSchema`.
//
// The avro.Deserializer has much of this functionality built in, other than being able to specify
// the `targetSchema` and instead infers the target schema from the target struct. This creates
// issues in some common use cases.
func (f AvroSchemaRegistryMarshaler) Unmarshal(req UnmarshReq) error {
	if f.deserSchema == "" {
		return errors.New("avro schema is required for schema registry marshaler: set SchemaRegistryConfig.Deserialization.Schema")
	}
	resolvedSchema, err := f.resolveSchema(req)
	if err != nil {
		return err
	}
	err = avro.Unmarshal(resolvedSchema, req.data[5:], req.target)
	if err != nil {
		return fmt.Errorf("failed to deserialize to confluent schema registry avro type: %w", err)
	}
	return nil
}

// resolveSchema returns the resolved (target<-data) avro schema for req. Results
// are cached by the message's confluent schema ID (bytes [1:5]) so the expensive
// GetSchema + Parse + compatibility Resolve runs once per schema ID rather than
// once per message.
func (f AvroSchemaRegistryMarshaler) resolveSchema(req UnmarshReq) (avro.Schema, error) {
	if len(req.data) < 5 {
		return nil, fmt.Errorf("payload of %d bytes is shorter than the 5 byte confluent schema registry header", len(req.data))
	}
	id := binary.BigEndian.Uint32(req.data[1:5])
	if cached, ok := f.resolvedSchemas.Load(id); ok {
		if schema, ok := cached.(avro.Schema); ok {
			return schema, nil
		}
	}

	inInfo, err := f.aMarshaler.deser.GetSchema(req.topic, req.data)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema from message payload: %w", err)
	}
	// schema of data that exists on the wire, that is about to be marshaled into the schema of our target
	dataSchema, err := avro.Parse(inInfo.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema associated with message: %w", err)
	}
	targetSchema, err := avro.Parse(f.deserSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema : %w", err)
	}
	resolvedSchema, err := avro.NewSchemaCompatibility().Resolve(targetSchema, dataSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to reconcile producer/consumer schemas: %w", err)
	}
	f.resolvedSchemas.Store(id, resolvedSchema)
	return resolvedSchema, nil
}

type AvroSchemaRegistryMarshalerFactory struct {
	srf                  *SchemaRegistryFactory
	schemaRegistryConfig SchemaRegistryConfig
}

// NewAvroSchemaRegistryMarshalerFactory returns a factory for marshalers which speak to confluent schema
// registry. Marshal uses schemaRegistryConfig.Serialization.Schema; Unmarshal resolves the on-the-wire
// schema against schemaRegistryConfig.Deserialization.Schema.
func NewAvroSchemaRegistryMarshalerFactory(schemaRegistryConfig SchemaRegistryConfig) AvroSchemaRegistryMarshalerFactory {
	return AvroSchemaRegistryMarshalerFactory{
		srf:                  newSchemaRegistryFactory(),
		schemaRegistryConfig: schemaRegistryConfig,
	}
}

func (f AvroSchemaRegistryMarshalerFactory) GetMarshaler() (KMarshaler, error) {
	aMarshaler, err := f.srf.createAvro(f.schemaRegistryConfig)
	if err != nil {
		return nil, err
	}
	return newAvroSchemaRegistryMarshaler(
		aMarshaler,
		f.schemaRegistryConfig.Serialization.Schema,
		f.schemaRegistryConfig.Deserialization.Schema,
	), nil
}

type ProtoSchemaRegistryMarshaler struct {
	pMarshaler protoMarshaler
}

func newProtoSchemaRegistryMarshaler(pMarshaler protoMarshaler) ProtoSchemaRegistryMarshaler {
	return ProtoSchemaRegistryMarshaler{
		pMarshaler: pMarshaler,
	}
}

func (f ProtoSchemaRegistryMarshaler) Marshal(req MarshReq) ([]byte, error) {
	msgBytes, err := f.pMarshaler.ser.Serialize(req.topic, req.v)
	if err != nil {
		return nil, fmt.Errorf("failed to proto serialize: %w", err)
	}
	return msgBytes, nil
}

func (f ProtoSchemaRegistryMarshaler) Unmarshal(req UnmarshReq) error {
	if err := f.pMarshaler.deser.DeserializeInto(req.topic, req.data, req.target); err != nil {
		return fmt.Errorf("failed to proto deserialize: %w", err)
	}
	return nil
}

type ProtoSchemaRegistryMarshalerFactory struct {
	srf                  *SchemaRegistryFactory
	schemaRegistryConfig SchemaRegistryConfig
}

func NewProtoSchemaRegistryMarshalerFactory(schemaRegistryConfig SchemaRegistryConfig) ProtoSchemaRegistryMarshalerFactory {
	return ProtoSchemaRegistryMarshalerFactory{
		srf:                  newSchemaRegistryFactory(),
		schemaRegistryConfig: schemaRegistryConfig,
	}
}

func (f ProtoSchemaRegistryMarshalerFactory) GetMarshaler() (KMarshaler, error) {
	pMarshaler, err := f.srf.createProto(f.schemaRegistryConfig)
	if err != nil {
		return nil, err
	}
	return newProtoSchemaRegistryMarshaler(pMarshaler), nil
}

type JsonSchemaRegistryMarshaler struct {
	jMarshaler jsonMarshaler
}

func newJsonSchemaRegistryMarshaler(jMarshaler jsonMarshaler) JsonSchemaRegistryMarshaler {
	return JsonSchemaRegistryMarshaler{
		jMarshaler: jMarshaler,
	}
}

func (f JsonSchemaRegistryMarshaler) Marshal(req MarshReq) ([]byte, error) {
	msgBytes, err := f.jMarshaler.ser.Serialize(req.topic, req.v)
	if err != nil {
		return nil, fmt.Errorf("failed to json schema serialize: %w", err)
	}
	return msgBytes, nil
}

func (f JsonSchemaRegistryMarshaler) Unmarshal(req UnmarshReq) error {
	if err := f.jMarshaler.deser.DeserializeInto(req.topic, req.data, req.target); err != nil {
		return fmt.Errorf("failed to json schema deserialize: %w", err)
	}
	return nil
}

type JsonSchemaRegistryMarshalerFactory struct {
	srf                  *SchemaRegistryFactory
	schemaRegistryConfig SchemaRegistryConfig
}

func NewJsonSchemaRegistryMarshalerFactory(schemaRegistryConfig SchemaRegistryConfig) JsonSchemaRegistryMarshalerFactory {
	return JsonSchemaRegistryMarshalerFactory{
		srf:                  newSchemaRegistryFactory(),
		schemaRegistryConfig: schemaRegistryConfig,
	}
}

func (f JsonSchemaRegistryMarshalerFactory) GetMarshaler() (KMarshaler, error) {
	jMarshaler, err := f.srf.createJson(f.schemaRegistryConfig)
	if err != nil {
		return nil, err
	}
	return newJsonSchemaRegistryMarshaler(jMarshaler), nil
}
