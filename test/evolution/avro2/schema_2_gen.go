// Code generated by avrogen. DO NOT EDIT.

package avro2

import (
	"github.com/heetch/avro/avrotypegen"
)

type DummyEvent struct {
	IntField            int
	DoubleField         float64
	StringField         string
	BoolField           bool
	BytesField          []byte
	NewFieldWithDefault *string
}

// AvroRecord implements the avro.AvroRecord interface.
func (DummyEvent) AvroRecord() avrotypegen.RecordInfo {
	return avrotypegen.RecordInfo{
		Schema: `{"fields":[{"name":"IntField","type":"int"},{"name":"DoubleField","type":"double"},{"name":"StringField","type":"string"},{"name":"BoolField","type":"boolean"},{"name":"BytesField","type":"bytes"},{"default":null,"name":"NewFieldWithDefault","type":["null","string"]}],"name":"DummyEvent","type":"record"}`,
		Required: []bool{
			0: true,
			1: true,
			2: true,
			3: true,
			4: true,
		},
	}
}
