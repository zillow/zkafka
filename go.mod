module github.com/zillow/zkafka/v2

go 1.24

require (
	github.com/confluentinc/confluent-kafka-go/v2 v2.10.1
	github.com/google/go-cmp v0.7.0
	github.com/google/uuid v1.6.0
	github.com/hamba/avro/v2 v2.29.0
	github.com/heetch/avro v0.4.79
	github.com/sony/gobreaker v1.0.0
	github.com/stretchr/testify v1.10.0
	github.com/zillow/zfmt/avro v0.0.0-00010101000000-000000000000
	github.com/zillow/zfmt/proto v0.0.0-00010101000000-000000000000
	go.opentelemetry.io/otel v1.37.0
	go.opentelemetry.io/otel/trace v1.37.0
	go.uber.org/mock v0.5.2
	golang.org/x/sync v0.15.0
	google.golang.org/protobuf v1.36.6
)

require (
	github.com/stoewer/go-strcase v1.3.0 // indirect
	github.com/zillow/zfmt v1.0.2-0.20250903212802-02f4f72d62df // indirect
	google.golang.org/api v0.183.0 // indirect
)

require (
	github.com/actgardner/gogen-avro/v10 v10.2.1 // indirect
	github.com/bahlo/generic-list-go v0.2.0 // indirect
	github.com/bufbuild/protocompile v0.14.1 // indirect
	github.com/buger/jsonparser v1.1.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-viper/mapstructure/v2 v2.3.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/invopop/jsonschema v0.13.0 // indirect
	github.com/jhump/protoreflect v1.17.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/mailru/easyjson v0.9.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/santhosh-tekuri/jsonschema/v5 v5.3.1 // indirect
	github.com/wk8/go-ordered-map/v2 v2.1.8 // indirect
	github.com/zillow/zfmt/json v0.0.0-20250912050045-707af315292f
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/otel/metric v1.37.0 // indirect
	golang.org/x/oauth2 v0.30.0 // indirect
	google.golang.org/genproto v0.0.0-20250603155806-513f23925822 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace (
	cloud.google.com/go => cloud.google.com/go v0.115.0
	github.com/zillow/zfmt => ../zfmt
	github.com/zillow/zfmt/avro => ../zfmt/avro
	github.com/zillow/zfmt/json => ../zfmt/json
	github.com/zillow/zfmt/proto => ../zfmt/proto
)
