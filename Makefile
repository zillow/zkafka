# Directories containing independent Go modules.
MODULE_DIRS = .

.PHONY: test-no-setup
test-no-setup:
	./coverage.sh

.PHONY: setup-test
setup-test:
	docker compose -p $$RANDOM -f ./example/compose.yaml up -d

.PHONY: test-local
test-local: setup-test test-no-setup

.PHONY: cover
cover:
	./coverage.sh

.PHONY: example-producer
example-producer:
	go run example/producer/producer.go

.PHONY: example-worker
example-worker:
	go run example/worker/worker.go

.PHONY: lint
lint: golangci-lint

.PHONY: golangci-lint
golangci-lint:
	@$(foreach mod,$(MODULE_DIRS), \
		(cd $(mod) && \
		echo "[lint] golangci-lint: $(mod)" && \
		golangci-lint run --path-prefix $(mod) ./...) &&) true
