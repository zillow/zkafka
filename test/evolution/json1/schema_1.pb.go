package json1

type DummyEvent struct {
	IntField    int64   `json:"IntField,omitempty"`
	DoubleField float32 `json:"DoubleField,omitempty"`
	StringField string  `json:"StringField,omitempty"`
	BoolField   bool    `json:"BoolField,omitempty"`
	BytesField  []byte  `json:"BytesField,omitempty"`
}
