package zkafka

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/zillow/zkafka/v3/test/evolution/proto1"
)

const (
	testAvroSchemaV1 = `{"type":"record","name":"TestEvent","namespace":"zkafka.test","fields":[{"name":"id","type":"string"},{"name":"count","type":"int"}]}`
	// testAvroSchemaV2 adds a defaulted field, a backward compatible evolution of V1
	testAvroSchemaV2 = `{"type":"record","name":"TestEvent","namespace":"zkafka.test","fields":[{"name":"id","type":"string"},{"name":"count","type":"int"},{"name":"note","type":"string","default":"n/a"}]}`
	// testAvroSchemaIncompatible changes the type of id, which cannot be resolved against V1
	testAvroSchemaIncompatible = `{"type":"record","name":"TestEvent","namespace":"zkafka.test","fields":[{"name":"id","type":"int"},{"name":"count","type":"int"}]}`
)

type testAvroEventV1 struct {
	ID    string `avro:"id"`
	Count int    `avro:"count"`
}

type testAvroEventV2 struct {
	ID    string `avro:"id"`
	Count int    `avro:"count"`
	Note  string `avro:"note"`
}

type testJSONEvent struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

// failingJSONEvent always fails to encode, to drive the serializer's error path
type failingJSONEvent struct{}

func (failingJSONEvent) MarshalJSON() ([]byte, error) {
	return nil, errors.New("boom")
}

func TestJSONMarshaler_RoundTrip(t *testing.T) {
	// arrange
	m := JSONMarshaler{}
	in := testJSONEvent{Name: "stewy", Age: 7}

	// act
	b, err := m.Marshal(in)
	require.NoError(t, err)
	var out testJSONEvent
	err = m.Unmarshal(b, &out)

	// assert
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"stewy","age":7}`, string(b))
	require.Equal(t, in, out)
}

func TestKMarshalerShim_IsItsOwnFactory(t *testing.T) {
	// arrange
	shim := KMarshalerShim{F: JSONMarshaler{}}

	// act
	km, err := shim.GetMarshaler()

	// assert
	require.NoError(t, err)
	require.Equal(t, shim, km)
}

func TestKMarshalerShim_DelegatesToWrappedMarshaler(t *testing.T) {
	// arrange
	shim := KMarshalerShim{F: JSONMarshaler{}}

	// act
	b, err := shim.Marshal(MarshReq{topic: "topic", v: testJSONEvent{Name: "lydia", Age: 3}})
	require.NoError(t, err)
	var out testJSONEvent
	err = shim.Unmarshal(UnmarshReq{topic: "topic", data: b, target: &out})

	// assert
	require.NoError(t, err)
	require.Equal(t, testJSONEvent{Name: "lydia", Age: 3}, out)
}

// avroRegistryFixture returns an avro marshaler whose serializer and deserializer share one mock
// schema registry, so values written with serSchema can be read back with deserSchema.
func avroRegistryFixture(t *testing.T, serSchema, deserSchema string, autoRegister bool) AvroSchemaRegistryMarshaler {
	t.Helper()
	srf := newSchemaRegistryFactory()
	aMarshaler, err := srf.createAvro(SchemaRegistryConfig{
		URL:           "mock://" + t.Name(),
		Serialization: SerializationConfig{AutoRegisterSchemas: autoRegister},
	})
	require.NoError(t, err)
	return newAvroSchemaRegistryMarshaler(aMarshaler, serSchema, deserSchema)
}

func TestNewAvroSchemaRegistryMarshalerFactory_GetMarshaler(t *testing.T) {
	// arrange
	factory := NewAvroSchemaRegistryMarshalerFactory(SchemaRegistryConfig{
		URL:             "mock://" + t.Name(),
		Serialization:   SerializationConfig{AutoRegisterSchemas: true, Schema: testAvroSchemaV1},
		Deserialization: DeserializationConfig{Schema: testAvroSchemaV2},
	})
	in := testAvroEventV1{ID: uuid.NewString(), Count: 4}

	// act
	km, err := factory.GetMarshaler()
	require.NoError(t, err)
	payload, err := km.Marshal(MarshReq{topic: "topic", v: in})
	require.NoError(t, err)
	var out testAvroEventV2
	err = km.Unmarshal(UnmarshReq{topic: "topic", data: payload, target: &out})

	// assert
	require.NoError(t, err)
	m, ok := km.(AvroSchemaRegistryMarshaler)
	require.True(t, ok)
	require.Equal(t, testAvroSchemaV1, m.serSchema)
	require.Equal(t, testAvroSchemaV2, m.deserSchema)
	require.Equal(t, testAvroEventV2{ID: in.ID, Count: 4, Note: "n/a"}, out)
}

func TestNewAvroSchemaRegistryMarshalerFactory_GetMarshaler_RequiresURL(t *testing.T) {
	// arrange
	factory := NewAvroSchemaRegistryMarshalerFactory(SchemaRegistryConfig{})

	// act
	km, err := factory.GetMarshaler()

	// assert
	require.Nil(t, km)
	require.ErrorContains(t, err, "no schema registry url provided")
}

func TestAvroSchemaRegistryMarshaler_Marshal_WritesConfluentWireFormat(t *testing.T) {
	// arrange
	m := avroRegistryFixture(t, testAvroSchemaV1, testAvroSchemaV1, true)
	in := testAvroEventV1{ID: "abc", Count: 1}

	// act
	payload, err := m.Marshal(MarshReq{topic: "topic", v: in})

	// assert
	require.NoError(t, err)
	require.Greater(t, len(payload), 5)
	require.Equal(t, byte(0), payload[0], "magic byte")
	require.NotZero(t, binary.BigEndian.Uint32(payload[1:5]), "schema id")
}

func TestAvroSchemaRegistryMarshaler_Marshal_RequiresSerializationSchema(t *testing.T) {
	// arrange
	m := avroRegistryFixture(t, "", testAvroSchemaV1, true)

	// act
	payload, err := m.Marshal(MarshReq{topic: "topic", v: testAvroEventV1{}})

	// assert
	require.Nil(t, payload)
	require.ErrorContains(t, err, "Serialization.Schema")
}

func TestAvroSchemaRegistryMarshaler_Marshal_UnregisteredSchemaWithoutAutoRegisterFails(t *testing.T) {
	// arrange
	m := avroRegistryFixture(t, testAvroSchemaV1, testAvroSchemaV1, false)

	// act
	payload, err := m.Marshal(MarshReq{topic: "topic", v: testAvroEventV1{ID: "abc"}})

	// assert
	require.Nil(t, payload)
	require.ErrorContains(t, err, "failed to get avro schema by id for topic topic")
}

func TestAvroSchemaRegistryMarshaler_Marshal_InvalidSchemaFails(t *testing.T) {
	// arrange
	m := avroRegistryFixture(t, `{"type":"record"`, testAvroSchemaV1, true)

	// act
	payload, err := m.Marshal(MarshReq{topic: "topic", v: testAvroEventV1{ID: "abc"}})

	// assert
	require.Nil(t, payload)
	require.Error(t, err)
}

func TestAvroSchemaRegistryMarshaler_Marshal_ValueNotMatchingSchemaFails(t *testing.T) {
	// arrange
	m := avroRegistryFixture(t, testAvroSchemaV1, testAvroSchemaV1, true)

	// act
	payload, err := m.Marshal(MarshReq{topic: "topic", v: "not a record"})

	// assert
	require.Nil(t, payload)
	require.ErrorContains(t, err, "failed to marhall payload per avro schema")
}

func TestAvroSchemaRegistryMarshaler_Unmarshal_ResolvesEvolvedSchemaAndCachesIt(t *testing.T) {
	// arrange
	m := avroRegistryFixture(t, testAvroSchemaV1, testAvroSchemaV2, true)
	in := testAvroEventV1{ID: "abc", Count: 9}
	payload, err := m.Marshal(MarshReq{topic: "topic", v: in})
	require.NoError(t, err)
	schemaID := binary.BigEndian.Uint32(payload[1:5])

	// act
	var first, second testAvroEventV2
	err1 := m.Unmarshal(UnmarshReq{topic: "topic", data: payload, target: &first})
	_, cachedAfterFirst := m.resolvedSchemas.Load(schemaID)
	err2 := m.Unmarshal(UnmarshReq{topic: "topic", data: payload, target: &second})

	// assert
	require.NoError(t, err1)
	require.NoError(t, err2)
	require.True(t, cachedAfterFirst, "resolved schema should be cached by schema id after first unmarshal")
	want := testAvroEventV2{ID: "abc", Count: 9, Note: "n/a"}
	require.Equal(t, want, first)
	require.Equal(t, want, second)
}

func TestAvroSchemaRegistryMarshaler_Unmarshal_RequiresDeserializationSchema(t *testing.T) {
	// arrange
	m := avroRegistryFixture(t, testAvroSchemaV1, "", true)
	payload, err := m.Marshal(MarshReq{topic: "topic", v: testAvroEventV1{ID: "abc"}})
	require.NoError(t, err)

	// act
	var out testAvroEventV1
	err = m.Unmarshal(UnmarshReq{topic: "topic", data: payload, target: &out})

	// assert
	require.ErrorContains(t, err, "Deserialization.Schema")
}

func TestAvroSchemaRegistryMarshaler_Unmarshal_UnknownSchemaIDFails(t *testing.T) {
	// arrange
	m := avroRegistryFixture(t, testAvroSchemaV1, testAvroSchemaV1, true)
	payload := []byte{0, 0, 0, 0, 99, 1, 2, 3}

	// act
	var out testAvroEventV1
	err := m.Unmarshal(UnmarshReq{topic: "topic", data: payload, target: &out})

	// assert
	require.ErrorContains(t, err, "failed to get schema from message payload")
}

func TestAvroSchemaRegistryMarshaler_Unmarshal_PayloadTooShortFails(t *testing.T) {
	// arrange
	m := avroRegistryFixture(t, testAvroSchemaV1, testAvroSchemaV1, true)

	// act
	var out testAvroEventV1
	err := m.Unmarshal(UnmarshReq{topic: "topic", data: []byte{0, 1}, target: &out})

	// assert
	require.ErrorContains(t, err, "shorter than the 5 byte confluent schema registry header")
}

func TestAvroSchemaRegistryMarshaler_Unmarshal_InvalidDeserializationSchemaFails(t *testing.T) {
	// arrange
	m := avroRegistryFixture(t, testAvroSchemaV1, `{"type":"record"`, true)
	payload, err := m.Marshal(MarshReq{topic: "topic", v: testAvroEventV1{ID: "abc"}})
	require.NoError(t, err)

	// act
	var out testAvroEventV1
	err = m.Unmarshal(UnmarshReq{topic: "topic", data: payload, target: &out})

	// assert
	require.ErrorContains(t, err, "failed to parse schema :")
}

func TestAvroSchemaRegistryMarshaler_Unmarshal_IncompatibleSchemasFail(t *testing.T) {
	// arrange
	m := avroRegistryFixture(t, testAvroSchemaV1, testAvroSchemaIncompatible, true)
	payload, err := m.Marshal(MarshReq{topic: "topic", v: testAvroEventV1{ID: "abc"}})
	require.NoError(t, err)

	// act
	var out testAvroEventV1
	err = m.Unmarshal(UnmarshReq{topic: "topic", data: payload, target: &out})

	// assert
	require.ErrorContains(t, err, "failed to reconcile producer/consumer schemas")
}

func TestAvroSchemaRegistryMarshaler_Unmarshal_CorruptBodyFails(t *testing.T) {
	// arrange
	m := avroRegistryFixture(t, testAvroSchemaV1, testAvroSchemaV1, true)
	payload, err := m.Marshal(MarshReq{topic: "topic", v: testAvroEventV1{ID: "a long enough id", Count: 1}})
	require.NoError(t, err)
	truncated := payload[:6]

	// act
	var out testAvroEventV1
	err = m.Unmarshal(UnmarshReq{topic: "topic", data: truncated, target: &out})

	// assert
	require.ErrorContains(t, err, "failed to deserialize to confluent schema registry avro type")
}

func TestProtoSchemaRegistryMarshaler_RoundTrip(t *testing.T) {
	// arrange
	factory := NewProtoSchemaRegistryMarshalerFactory(SchemaRegistryConfig{
		URL:           "mock://" + t.Name(),
		Serialization: SerializationConfig{AutoRegisterSchemas: true},
	})
	in := &proto1.DummyEvent{IntField: 42, StringField: "hello", BoolField: true}

	// act
	km, err := factory.GetMarshaler()
	require.NoError(t, err)
	payload, err := km.Marshal(MarshReq{topic: "topic", v: in})
	require.NoError(t, err)
	out := &proto1.DummyEvent{}
	err = km.Unmarshal(UnmarshReq{topic: "topic", data: payload, target: out})

	// assert
	require.NoError(t, err)
	require.Equal(t, in.GetIntField(), out.GetIntField())
	require.Equal(t, in.GetStringField(), out.GetStringField())
	require.Equal(t, in.GetBoolField(), out.GetBoolField())
}

func TestProtoSchemaRegistryMarshaler_Errors(t *testing.T) {
	// arrange
	factory := NewProtoSchemaRegistryMarshalerFactory(SchemaRegistryConfig{
		URL:           "mock://" + t.Name(),
		Serialization: SerializationConfig{AutoRegisterSchemas: true},
	})
	km, err := factory.GetMarshaler()
	require.NoError(t, err)

	// act
	payload, marshalErr := km.Marshal(MarshReq{topic: "topic", v: "not a proto message"})
	out := &proto1.DummyEvent{}
	unmarshalErr := km.Unmarshal(UnmarshReq{topic: "topic", data: []byte{0, 0, 0, 0, 99, 1}, target: out})

	// assert
	require.Nil(t, payload)
	require.ErrorContains(t, marshalErr, "failed to proto serialize")
	require.ErrorContains(t, unmarshalErr, "failed to proto deserialize")
}

func TestNewProtoSchemaRegistryMarshalerFactory_GetMarshaler_RequiresURL(t *testing.T) {
	// arrange
	factory := NewProtoSchemaRegistryMarshalerFactory(SchemaRegistryConfig{})

	// act
	km, err := factory.GetMarshaler()

	// assert
	require.Nil(t, km)
	require.ErrorContains(t, err, "no schema registry url provided")
}

func TestJsonSchemaRegistryMarshaler_RoundTrip(t *testing.T) {
	// arrange
	factory := NewJsonSchemaRegistryMarshalerFactory(SchemaRegistryConfig{
		URL:           "mock://" + t.Name(),
		Serialization: SerializationConfig{AutoRegisterSchemas: true},
	})
	in := testJSONEvent{Name: "asif", Age: 30}

	// act
	km, err := factory.GetMarshaler()
	require.NoError(t, err)
	payload, err := km.Marshal(MarshReq{topic: "topic", v: in})
	require.NoError(t, err)
	var out testJSONEvent
	err = km.Unmarshal(UnmarshReq{topic: "topic", data: payload, target: &out})

	// assert
	require.NoError(t, err)
	require.Equal(t, byte(0), payload[0], "magic byte")
	require.Equal(t, in, out)
}

func TestJsonSchemaRegistryMarshaler_Errors(t *testing.T) {
	// arrange
	factory := NewJsonSchemaRegistryMarshalerFactory(SchemaRegistryConfig{
		URL:           "mock://" + t.Name(),
		Serialization: SerializationConfig{AutoRegisterSchemas: true},
	})
	km, err := factory.GetMarshaler()
	require.NoError(t, err)

	// act
	payload, marshalErr := km.Marshal(MarshReq{topic: "topic", v: failingJSONEvent{}})
	var out testJSONEvent
	unmarshalErr := km.Unmarshal(UnmarshReq{topic: "topic", data: []byte{0, 0, 0, 0, 99, 1}, target: &out})

	// assert
	require.Nil(t, payload)
	require.ErrorContains(t, marshalErr, "failed to json schema serialize")
	require.ErrorContains(t, unmarshalErr, "failed to json schema deserialize")
}

func TestNewJsonSchemaRegistryMarshalerFactory_GetMarshaler_RequiresURL(t *testing.T) {
	// arrange
	factory := NewJsonSchemaRegistryMarshalerFactory(SchemaRegistryConfig{})

	// act
	km, err := factory.GetMarshaler()

	// assert
	require.Nil(t, km)
	require.ErrorContains(t, err, "no schema registry url provided")
}
