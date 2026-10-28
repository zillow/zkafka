# Directories containing independent Go modules.
MODULE_DIRS = . example
GOLANGCI_VERSION=2.11.4
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
	cd example && go run ./producer

.PHONY: example-worker
example-worker:
	cd example && go run ./worker

.PHONY: example-deadletter-worker
example-deadletter-worker:
	cd example && go run ./worker-deadletter

.PHONY: example-delay-worker
example-delay-worker:
	cd example && go run ./worker-delay

.PHONY: lint
lint: golangci-lint

.PHONY: golangci-lint
golangci-lint:
	@$(foreach mod,$(MODULE_DIRS), \
		(cd $(mod) && \
		echo "[lint] golangci-lint: $(mod)" && \
		go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v${GOLANGCI_VERSION} run $(ARGS) --path-prefix $(mod) ./...) &&) true

.PHONY: golangci-migrate
golangci-migrate:
		go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v${GOLANGCI_VERSION} migrate

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