package zkafka

import (
	"testing"

	"github.com/stretchr/testify/require"
	aryeopem1 "github.com/zillow/zkafka/v2/aryeo_pem_gen1"
	aryeopem2 "github.com/zillow/zkafka/v2/aryeo_pem_gen2"
)

func TestABC(t *testing.T) {

	f1 := newSchemaRegistryFactory()
	cfg1 := SchemaRegistryConfig{
		URL: "https://schema-registry.shared.zg-int.net:443",
		Serialization: SerializationConfig{
			AutoRegisterSchemas: false,
			Schema:              aryeopem1.Schema,
		},
		Deserialization: DeserializationConfig{
			Schema: aryeopem1.Schema,
		},
		SubjectName: "com.zillowgroup.rmx.pem_schema.AryeoMediaDelivered",
	}
	formatter1, err := f1.createAvro(cfg1)
	require.NoError(t, err)

	xformatter1, err := newAvroSchemaRegistryFormatter(formatter1)
	require.NoError(t, err)

	f2 := newSchemaRegistryFactory()
	cfg2 := SchemaRegistryConfig{
		URL: "https://schema-registry.shared.zg-int.net:443",
		Serialization: SerializationConfig{
			AutoRegisterSchemas: false,
			Schema:              aryeopem2.Schema,
		},
		Deserialization: DeserializationConfig{
			Schema: aryeopem2.Schema,
		},
		SubjectName: "com.zillowgroup.rmx.pem_schema.AryeoMediaDelivered",
	}
	formatter2, err := f2.createAvro(cfg2)
	require.NoError(t, err)
	xformatter2, err := newAvroSchemaRegistryFormatter(formatter2)
	require.NoError(t, err)

	u := "https://www.vr1.test-automation.zillow.net/view-imx/5c206afd-ab9b-4f23-aeac-0abe3ab0bf1c?setAttribution=mls&wl=true&initialViewType=pano&utm_source=dashboard"
	data1 := aryeopem1.AryeoMediaDeliveredRecord{
		EventID:   "70d8bb85-3c48-4e2f-b531-b4d536acab82",
		EventType: "MediaDelivered",
		CompanyID: "70d8bb85-3c48-4e2f-b531-b4d536acab82",
		ListingID: "70d8bb85-3c48-4e2f-b531-b4d536acab82",
		Address: aryeopem1.AddressRecord{
			Latitude:        41.9104057,
			Longitude:       -88.3120465,
			StreetNumber:    "402",
			StreetName:      "Brownstone Dr",
			UnitNumber:      nil,
			PostalCode:      "60174",
			City:            "Saint Charles",
			StateOrProvince: "IL",
			Country:         "USA",
		},
		InteractiveContent: &[]aryeopem1.InteractiveContentRecord{
			{
				URL: u,
			},
		},
	}

	topic := "xxxx"
	sdata1, err := xformatter1.marshall(marshReq{
		topic:  topic,
		v:      &data1,
		schema: cfg1.Serialization.Schema,
	})
	require.NoError(t, err)

	target := aryeopem2.AryeoMediaDeliveredRecord{}
	err = xformatter2.unmarshal(unmarshReq{
		topic:  topic,
		data:   sdata1,
		target: &target,
		schema: cfg2.Deserialization.Schema,
	})
	require.NoError(t, err)

	require.Equal(t, data1.EventType, target.EventType)
	require.NotNil(t, target.InteractiveContent)
	require.NotEmpty(t, *target.InteractiveContent)
	require.Equal(t, u, (*target.InteractiveContent)[0].URL)
	require.Nil(t, (*target.InteractiveContent)[0].IsImx)
}
