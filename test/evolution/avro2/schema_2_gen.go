package avro2

// Code generated by avro/gen. DO NOT EDIT.

import (
	"time"
)

// AddressRecord is a generated struct.
type AddressRecord struct {
	StreetNumber    string  `avro:"streetNumber"`
	StreetName      string  `avro:"streetName"`
	UnitNumber      *string `avro:"unitNumber"`
	PostalCode      string  `avro:"postalCode"`
	City            string  `avro:"city"`
	StateOrProvince string  `avro:"stateOrProvince"`
	Country         string  `avro:"country"`
	Latitude        float32 `avro:"latitude"`
	Longitude       float32 `avro:"longitude"`
}

// ImageRecord is a generated struct.
type ImageRecord struct {
	ID      string  `avro:"id"`
	Index   int     `avro:"index"`
	Caption *string `avro:"caption"`
	URL     *string `avro:"url"`
}

// AryeoFloorplanRecord is a generated struct.
type AryeoFloorplanRecord struct {
	URL string `avro:"url"`
	// Id(uuid v7) of the underlying Aryeo Floorplan Record.
	ID string `avro:"id"`
}

// InteractiveContentRecord is a generated struct.
type InteractiveContentRecord struct {
	URL string `avro:"url"`
}

// OrderRecord is a generated struct.
type OrderRecord struct {
	// Id(uuid v7) of the Aryeo Listing order.
	ID    string `avro:"id"`
	IsImx bool   `avro:"isImx"`
}

// AryeoListingRecord is a generated struct.
type AryeoListingRecord struct {
	// Id(uuid v7) of the underlying Aryeo Listing.
	ID string `avro:"id"`
	// Id(uuid v7) of Aryeo Listing's underlying Company.
	CompanyID string `avro:"companyId"`
	// Timestamp depicting when Aryeo's listing was delivered.
	DeliveredAtDateTimeUtc time.Time `avro:"deliveredAtDateTimeUtc"`
	// Enum depicting type of event.
	EventType string `avro:"eventType"`
	// Unique identifier of Showcase Engagement associated to underlying Aryeo Listing.
	EngagementID *string `avro:"engagementId"`
	// Address associated to Aryeo Listing.
	Address AddressRecord `avro:"address"`
	// Photos delivered for underlying Aryeo Listing.
	Images *[]ImageRecord `avro:"images"`
	// Floorplans associated to Aryeo Listing.
	Floorplans *[]AryeoFloorplanRecord `avro:"floorplans"`
	// InteractiveContent associated to Aryeo Listing.
	InteractiveContent *[]InteractiveContentRecord `avro:"interactiveContent"`
	// Orders associated to Aryeo Listing.
	Orders *[]OrderRecord `avro:"orders"`
}
