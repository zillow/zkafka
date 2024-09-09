# Directories containing independent Go modules.
MODULE_DIRS = .


# Sets up kafka broker using docker compose
.PHONY: setup
setup:
	docker compose -f ./example/compose.yaml up -d

# Assumes setup has been executed. Runs go test with coverage
.PHONY: cover
cover:
	./coverage.sh

# Runs setup and executes tests with coverage.
.PHONY: test-local
test-local: setup cover

.PHONY: example-producer
example-producer:
	go run example/producer/main.go

.PHONY: example-worker
example-worker:
	go run example/worker/main.go

.PHONY: example-deadletter-worker
example-deadletter-worker:
	go run example/worker-deadletter/main.go

.PHONY: example-delay-worker
example-delay-worker:
	go run example/worker-delay/main.go

.PHONY: lint
lint: golangci-lint

.PHONY: golangci-lint
golangci-lint:
	@$(foreach mod,$(MODULE_DIRS), \
		(cd $(mod) && \
		echo "[lint] golangci-lint: $(mod)" && \
		golangci-lint run --path-prefix $(mod) ./...) &&) true

.PHONY: gen
gen:
	go run github.com/heetch/avro/cmd/avrogo@v0.4.5 -p main -d ./example/producer_avro ./example/producer_avro/dummy_event.avsc
	go run github.com/heetch/avro/cmd/avrogo@v0.4.5 -p main -d ./example/worker_avro ./example/worker_avro/dummy_event.avsc
	go run github.com/heetch/avro/cmd/avrogo@v0.4.5 -p heetch1 -d ./test/heetch1 ./test/dummy_event_1.avsc
	go run github.com/heetch/avro/cmd/avrogo@v0.4.5 -p heetch2 -d ./test/heetch2 ./test/dummy_event_2.avsc
	#go run github.com/hamba/avro/v2/cmd/avrogen@v2.25.1 -pkg main -o ./example/producer_avro/bla.go  -tags json:snake,yaml:upper-camel ./example/producer_avro/dummy_event.avsc
	#go run github.com/hamba/avro/v2/cmd/avrogen@v2.25.1 -pkg main -o ./example/worker_avro/bla.go  -tags json:snake,yaml:upper-camel ./example/worker_avro/dummy_event.avsc
