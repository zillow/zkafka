# Directories containing independent Go modules.
MODULE_DIRS = .


# Sets up kafka broker using docker compose
.PHONY: setup
setup:
	docker compose -f ./example/compose.yaml up -d

# Assumes setup has been executed. Runs go test with coverage
.PHONY: cover
cover:
	export GO_TAGS=--tags=integration; ./coverage.sh

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
