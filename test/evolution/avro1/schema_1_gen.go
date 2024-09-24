package avro1

// Code generated by avro/gen. DO NOT EDIT.

import (
	"time"
)

// AryeoListingRecord is a generated struct.
type AryeoListingRecord struct {
	// Id(uuid v7) of the underlying Aryeo Listing.
	ID string `avro:"id"`
	// Timestamp depicting when Aryeo's listing was delivered.
	DeliveredAtDateTimeUtc time.Time `avro:"deliveredAtDateTimeUtc"`
	// Enum depicting type of event.
	EventType string `avro:"eventType"`
}
