// Code generated by avrogen. DO NOT EDIT.

package avro1

import (
	"fmt"
	"strconv"

	"github.com/heetch/avro/avrotypegen"
)

type Event struct {
	Id                     string    `json:"id"`
	Deliveredatdatetimeutc int64     `json:"deliveredAtDateTimeUtc"`
	Eventtype              EventType `json:"eventType"`
}

// AvroRecord implements the avro.AvroRecord interface.
func (Event) AvroRecord() avrotypegen.RecordInfo {
	return avrotypegen.RecordInfo{
		Schema: `{"fields":[{"name":"id","type":"string"},{"name":"deliveredAtDateTimeUtc","type":{"logicalType":"timestamp-millis","type":"long"}},{"name":"eventType","type":{"default":"created","name":"EventType","symbols":["created","associated"],"type":"enum"}}],"name":"com.zillowgroup.Event","type":"record"}`,
		Required: []bool{
			0: true,
			1: true,
			2: true,
		},
	}
}

type EventType int

const (
	EventTypeCreated EventType = iota
	EventTypeAssociated
)

var _EventType_strings = []string{
	"created",
	"associated",
}

// String returns the textual representation of EventType.
func (e EventType) String() string {
	if e < 0 || int(e) >= len(_EventType_strings) {
		return "EventType(" + strconv.FormatInt(int64(e), 10) + ")"
	}
	return _EventType_strings[e]
}

// MarshalText implements encoding.TextMarshaler
// by returning the textual representation of EventType.
func (e EventType) MarshalText() ([]byte, error) {
	if e < 0 || int(e) >= len(_EventType_strings) {
		return nil, fmt.Errorf("EventType value %d is out of bounds", e)
	}
	return []byte(_EventType_strings[e]), nil
}

// UnmarshalText implements encoding.TextUnmarshaler
// by expecting the textual representation of EventType.
func (e *EventType) UnmarshalText(data []byte) error {
	// Note for future: this could be more efficient.
	for i, s := range _EventType_strings {
		if string(data) == s {
			*e = EventType(i)
			return nil
		}
	}
	return fmt.Errorf("unknown value %q for EventType", data)
}