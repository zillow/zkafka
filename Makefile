# Directories containing independent Go modules.
MODULE_DIRS = .
GOLANGCI_VERSION=1.64.6
AVRO_CMD_PATH=github.com/hamba/avro/v2/cmd/avrogen@v2.28.0


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
		go run github.com/golangci/golangci-lint/cmd/golangci-lint@v${GOLANGCI_VERSION} run $(ARGS) --path-prefix $(mod) ./...) &&) true

.PHONY: gen
gen: protoc-exists
	cd test/evolution; protoc --proto_path=. --go_out=./ ./schema_1.proto
	cd test/evolution; protoc --proto_path=. --go_out=./ ./schema_2.proto
	go run ${AVRO_CMD_PATH} -pkg main -o ./example/producer_avro/event_gen.go   ./example/producer_avro/event.avsc
	go run ${AVRO_CMD_PATH} -pkg main -o ./example/worker_avro/event_gen.go   ./example/worker_avro/event.avsc
	mkdir -p ./test/evolution/avro1
	mkdir -p ./test/evolution/avro2
	go run ${AVRO_CMD_PATH} -pkg avro1 -o ./test/evolution/avro1/schema_1_gen.go ./test/evolution/schema_1.avsc
	go run ${AVRO_CMD_PATH} -pkg avro2 -o ./test/evolution/avro2/schema_2_gen.go ./test/evolution/schema_2.avsc
	go run github.com/heetch/avro/cmd/avrogo@v0.4.5 -p avro1 -d ./test/evolution/avro1x ./test/evolution/schema_1.avsc

# a forced dependency which fails (and prints) if `avro-tools` isn't installed
.PHONY: protoc-exists
protoc-exists:
	@which protoc > /dev/null || (echo "protoc is not installed. Install via `brew install protobuf`"; exit 1)