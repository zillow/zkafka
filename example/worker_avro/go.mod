module github.com/zillow/zkafka/example/worker_avro

go 1.23.1

replace github.com/zillow/zkafka v1.0.0 => ../..

require (
	github.com/google/uuid v1.6.0
)

require (
	github.com/actgardner/gogen-avro/v10 v10.2.1 // indirect
	github.com/confluentinc/confluent-kafka-go/v2 v2.5.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/heetch/avro v0.4.5 // indirect
	github.com/sony/gobreaker v1.0.0 // indirect
	github.com/zillow/zfmt v1.0.1 // indirect
	go.opentelemetry.io/otel v1.28.0 // indirect
	go.opentelemetry.io/otel/trace v1.28.0 // indirect
	golang.org/x/sync v0.7.0 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
)
