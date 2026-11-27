GO ?= go
SEEDS ?= 10000

.PHONY: test test-race vet fmt sim sim-raft stress-raft golden-stage2

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

sim-raft:
	$(GO) run ./cmd/simctl -seed 1 -scenario raft -ticks 400 -trace

stress-raft:
	$(GO) test ./internal/scenarios -run TestRaftMultiSeed -count=1 \
		-timeout 60m -args -raft-seeds=$(SEEDS)

golden-stage2:
	$(GO) test ./internal/scenarios -run TestRaftGoldenTraceSeed42 -update
