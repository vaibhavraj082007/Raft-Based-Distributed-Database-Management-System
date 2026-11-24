GO ?= go

.PHONY: test test-race vet fmt sim

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -w .

sim:
	$(GO) run ./cmd/simctl -seed 1 -scenario ping -trace
