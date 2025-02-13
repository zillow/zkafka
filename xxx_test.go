package zkafka

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestABC(t *testing.T) {

	f := newSchemaRegistryFactory()
	cfg := SchemaRegistryConfig{
		URL:             "https://schema-registry.shared.zg-int.net:443",
		Serialization:   SerializationConfig{},
		Deserialization: DeserializationConfig{},
		SubjectName:     "",
	}
	formatter1, err := f.createAvro(cfg)
	require.NoError(t, err)

}
