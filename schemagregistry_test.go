package zkafka

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/stretchr/testify/require"
)

func Test_subjectNameStrategy_UsesConfiguredSubjectName(t *testing.T) {
	// arrange
	strategy := subjectNameStrategy(SchemaRegistryConfig{SubjectName: "my-subject"})

	// act
	subject, err := strategy("some-topic", serde.ValueSerde, schemaregistry.SchemaInfo{})

	// assert
	require.NoError(t, err)
	require.Equal(t, "my-subject", subject)
}

func Test_subjectNameStrategy_DefaultsToTopicNameStrategy(t *testing.T) {
	// arrange
	strategy := subjectNameStrategy(SchemaRegistryConfig{})

	// act
	subject, err := strategy("some-topic", serde.ValueSerde, schemaregistry.SchemaInfo{})

	// assert
	require.NoError(t, err)
	require.Equal(t, "some-topic-value", subject)
}

func Test_SchemaRegistryFactory_getSchemaClient_RequiresURL(t *testing.T) {
	// arrange
	srf := newSchemaRegistryFactory()

	// act
	cl, err := srf.getSchemaClient(SchemaRegistryConfig{})

	// assert
	require.Nil(t, cl)
	require.ErrorContains(t, err, "no schema registry url provided")
}

func Test_SchemaRegistryFactory_getSchemaClient_CachesClientPerURL(t *testing.T) {
	// arrange
	srf := newSchemaRegistryFactory()

	// act
	first, err := srf.getSchemaClient(SchemaRegistryConfig{URL: "mock://cache-a"})
	require.NoError(t, err)
	second, err := srf.getSchemaClient(SchemaRegistryConfig{URL: "mock://cache-a"})
	require.NoError(t, err)
	other, err := srf.getSchemaClient(SchemaRegistryConfig{URL: "mock://cache-b"})
	require.NoError(t, err)

	// assert
	require.Same(t, first, second)
	require.NotSame(t, first, other)
	require.Len(t, srf.srCls, 2)
}

func Test_SchemaRegistryFactory_create_PropagatesClientError(t *testing.T) {
	// arrange
	srf := newSchemaRegistryFactory()
	cfg := SchemaRegistryConfig{}

	// act
	_, avroErr := srf.createAvro(cfg)
	_, protoErr := srf.createProto(cfg)
	_, jsonErr := srf.createJson(cfg)

	// assert
	require.ErrorContains(t, avroErr, "no schema registry url provided")
	require.ErrorContains(t, protoErr, "no schema registry url provided")
	require.ErrorContains(t, jsonErr, "no schema registry url provided")
}

func Test_SchemaRegistryFactory_create_BuildsSerializerAndDeserializer(t *testing.T) {
	// arrange
	srf := newSchemaRegistryFactory()
	cfg := SchemaRegistryConfig{
		URL:           "mock://create",
		Serialization: SerializationConfig{AutoRegisterSchemas: true},
	}

	// act
	avroM, avroErr := srf.createAvro(cfg)
	protoM, protoErr := srf.createProto(cfg)
	jsonM, jsonErr := srf.createJson(cfg)

	// assert
	require.NoError(t, avroErr)
	require.NotNil(t, avroM.ser)
	require.NotNil(t, avroM.deser)
	require.True(t, avroM.ser.Conf.AutoRegisterSchemas)
	require.NoError(t, protoErr)
	require.NotNil(t, protoM.ser)
	require.NotNil(t, protoM.deser)
	require.True(t, protoM.ser.Conf.AutoRegisterSchemas)
	require.NoError(t, jsonErr)
	require.NotNil(t, jsonM.ser)
	require.NotNil(t, jsonM.deser)
	require.True(t, jsonM.ser.Conf.AutoRegisterSchemas)
}
