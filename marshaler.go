package zkafka

import (
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	"github.com/hamba/avro/v2"
)

var errMissingMarshaler = errors.New("custom marshaler is missing, did you forget to call WithMarshaler() ?")

type MarshReq struct {
	// topic is the kafka topic being written to
	topic string
	// v is the data to be marshalled
	v any
}

type UnmarshReq struct {
	// topic is the kafka topic being read from
	topic string
	// data is the message value which will be unmarshalled to a type
	data []byte
	// target is the stuct which is to be hydrated by the contents of data
	target any
}

var _ KMarshaler = (*AvroSchemaRegistryMarshaler)(nil)
var _ KMarshaler = (*ProtoSchemaRegistryMarshaler)(nil)
var _ KMarshaler = (*JsonSchemaRegistryMarshaler)(nil)

// KMarshaler is a zkafka special marshaler.
type KMarshaler interface {
	Marshall(req MarshReq) ([]byte, error)
	Unmarshal(req UnmarshReq) error
}

type KMarshalerFactory interface {
	GetMarshaler() (KMarshaler, error)
}

// Marshaler is a generic marshaler interface
type Marshaler interface {
	Marshall(v any) ([]byte, error)
	Unmarshal(b []byte, v any) error
}

// KMarshalerShim is a shim type which allows
// generic marshalers to work as KMarshalers
type KMarshalerShim struct {
	F Marshaler
}

func (f KMarshalerShim) Marshall(req MarshReq) ([]byte, error) {
	return f.F.Marshall(req.v)
}

func (f KMarshalerShim) Unmarshal(req UnmarshReq) error {
	return f.F.Unmarshal(req.data, req.target)
}

// KMarshalerFactoryShim is a shim type which allows
// a generic marshaler to work as KMarshalerFactory
type KMarshalerFactoryShim struct {
	F Marshaler
}

func (f KMarshalerFactoryShim) GetMarshaler() (KMarshaler, error) {
	return KMarshalerShim{F: f.F}, nil
}

type avroMarshaler struct {
	ser   *avrov2.Serializer
	deser *avrov2.Deserializer
}

func (s avroMarshaler) GetID(topic string, avroSchema string) (int, error) {
	return s.ser.GetID(topic, nil, &schemaregistry.SchemaInfo{Schema: avroSchema})
}

func (s avroMarshaler) Deserialize(topic string, value []byte, target any) error {
	return s.deser.DeserializeInto(topic, value, target)
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
	schema     string
	aMarshaler avroMarshaler
}

func newAvroSchemaRegistryMarshaler(
	aMarshaler avroMarshaler,
	schema string,
) AvroSchemaRegistryMarshaler {
	return AvroSchemaRegistryMarshaler{
		schema:     schema,
		aMarshaler: aMarshaler,
	}
}

// marshall looks a subject's schema (id) so that it can prefix the eventual message payload.
// A schema must be provided and hamba/avro is used in conjunction with this schema to marshall they payload.
// Structs generated using hamba/avro work best, since they provide avro tags which handles casing
// which can lead to errors otherwise.
func (f AvroSchemaRegistryMarshaler) Marshall(req MarshReq) ([]byte, error) {
	if f.schema == "" {
		return nil, errors.New("avro schema is required for schema registry marshaler")
	}
	id, err := f.aMarshaler.GetID(req.topic, f.schema)
	if err != nil {
		return nil, fmt.Errorf("failed to get avro schema by id for topic %s: %w", req.topic, err)
	}
	avroSchema, err := avro.Parse(f.schema)
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

// unmarshall looks up the schema based on the schemaID in the message payload (`dataSchema`).
// Additionally, a target schema is provided in the request (`targetSchema`).
//
// The 'targetSchema' and 'dataSchema' are resolved so that data written by the `dataSchema` may
// be read by the `targetSchema`.
//
// The avro.Deserializer has much of this functionality built in, other than being able to specify
// the `targetSchema` and instead infers the target schema from the target struct. This creates
// issues in some common use cases.
func (f AvroSchemaRegistryMarshaler) Unmarshal(req UnmarshReq) error {
	if f.schema == "" {
		return errors.New("avro schema is required for schema registry marshaler")
	}
	inInfo, err := f.aMarshaler.deser.GetSchema(req.topic, req.data)
	if err != nil {
		return fmt.Errorf("failed to get schema from message payload: %w", err)
	}

	// schema of data that exists on the wire, that is about to be marshalled into the schema of our target
	dataSchema, err := avro.Parse(inInfo.Schema)
	if err != nil {
		return fmt.Errorf("failed to parse schema associated with message: %w", err)
	}

	targetSchema, err := avro.Parse(f.schema)
	if err != nil {
		return fmt.Errorf("failed to parse schema : %w", err)
	}
	sc := avro.NewSchemaCompatibility()

	resolvedSchema, err := sc.Resolve(targetSchema, dataSchema)
	if err != nil {
		return fmt.Errorf("failed to reconcile producer/consumer schemas: %w", err)
	}
	err = avro.Unmarshal(resolvedSchema, req.data[5:], req.target)
	if err != nil {
		return fmt.Errorf("failed to deserialize to confluent schema registry avro type: %w", err)
	}
	return nil
}

type AvroSchemaRegistryMarshalerFactory struct {
	schema               string
	srf                  *SchemaRegistryFactory
	schemaRegistryConfig SchemaRegistryConfig
}

func NewAvroSchemaRegistryMarshalerFactory(schemaRegistryConfig SchemaRegistryConfig) AvroSchemaRegistryMarshalerFactory {
	return AvroSchemaRegistryMarshalerFactory{
		schema:               schemaRegistryConfig.Serialization.Schema,
		srf:                  newSchemaRegistryFactory(),
		schemaRegistryConfig: schemaRegistryConfig,
	}
}

func (f AvroSchemaRegistryMarshalerFactory) GetMarshaler() (KMarshaler, error) {
	aMarshaler, err := f.srf.createAvro(f.schemaRegistryConfig)
	if err != nil {
		return nil, err
	}
	return newAvroSchemaRegistryMarshaler(aMarshaler, f.schema), nil
}

type ProtoSchemaRegistryMarshaler struct {
	pMarshaler protoMarshaler
}

func newProtoSchemaRegistryMarshaler(pMarshaler protoMarshaler) ProtoSchemaRegistryMarshaler {
	return ProtoSchemaRegistryMarshaler{
		pMarshaler: pMarshaler,
	}
}

func (f ProtoSchemaRegistryMarshaler) Marshall(req MarshReq) ([]byte, error) {
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

func (f JsonSchemaRegistryMarshaler) Marshall(req MarshReq) ([]byte, error) {
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
