.PHONY: test-no-setup
test-no-setup:
	./coverage.sh

.PHONY: setup-test
setup-test:
	docker compose -p $$RANDOM -f ./example/docker-compose.yaml up -d

.PHONY: test-local
test-local: setup-test test-no-setup

.PHONY: cover
cover:
	go test -v ./... -count=1 -coverprofile=cover.out -covermode atomic && \
	go tool cover -html=cover.out -o cover.html

.PHONY: example-producer
example-producer:
	go run example/producer/producer.go

.PHONY: example-worker
example-worker:
	go run example/worker/worker.go