# Kimistore -- build, test and operational targets.
#
# Everything CI runs is here, so a check that exists only on a laptop cannot
# quietly rot. `make ci` is what the workflow runs; `make e2e` is the one check
# that needs Docker and is therefore kept out of the fast path.

SHELL := /bin/bash
.DEFAULT_GOAL := help

AGENT      := agent
BIN_DIR    := bin
COVER_OUT  := coverage.out
COVER_HTML := coverage.html

# -timeout guards against a hung test rather than letting CI sit until the job
# limit; the storage tests deliberately exercise bounded shutdown paths.
TEST_FLAGS := -count=1 -timeout 300s

GO       ?= go
GOFILES  := $(shell find . -name '*.go' -not -path './.git/*')

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

## ---------------------------------------------------------------- build

.PHONY: build
build: ## Build the agent binary
	$(GO) build -o $(AGENT) ./cmd/agent

.PHONY: build-tools
build-tools: ## Build the benchmark and load-test tools
	$(GO) build -o $(BIN_DIR)/benchmark ./cmd/benchmark
	$(GO) build -o $(BIN_DIR)/load-test ./cmd/load-test

.PHONY: run
run: build ## Build and run the agent
	./$(AGENT)

.PHONY: clean
clean: ## Remove build output and coverage artefacts
	rm -rf $(AGENT) $(BIN_DIR) $(COVER_OUT) $(COVER_HTML) coverage

## ---------------------------------------------------------------- checks

.PHONY: fmt
fmt: ## Format the tree
	gofmt -w $(GOFILES)

.PHONY: fmt-check
fmt-check: ## Fail if anything is unformatted
	@unformatted=$$(gofmt -l $(GOFILES)); \
	if [ -n "$$unformatted" ]; then \
		echo "not gofmt'd:"; echo "$$unformatted"; exit 1; \
	fi

.PHONY: vet
vet: ## Run go vet
	$(GO) vet ./...

.PHONY: lint
lint: ## Run golangci-lint (falls back to vet when it is not installed)
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed; falling back to go vet"; \
		$(MAKE) vet; \
	fi

.PHONY: tidy-check
tidy-check: ## Fail if go.mod/go.sum are not tidy
	@cp go.mod go.mod.bak && cp go.sum go.sum.bak
	@$(GO) mod tidy
	@if ! cmp -s go.mod go.mod.bak || ! cmp -s go.sum go.sum.bak; then \
		mv go.mod.bak go.mod; mv go.sum.bak go.sum; \
		echo "go.mod/go.sum are not tidy; run 'go mod tidy'"; exit 1; \
	fi
	@rm -f go.mod.bak go.sum.bak

.PHONY: test
test: ## Run the unit tests
	$(GO) test $(TEST_FLAGS) ./...

.PHONY: race
race: ## Run the unit tests under the race detector
	$(GO) test -race $(TEST_FLAGS) ./...

.PHONY: cover
cover: ## Run tests with coverage and write coverage.html
	$(GO) test $(TEST_FLAGS) -coverprofile=$(COVER_OUT) -covermode=atomic ./...
	$(GO) tool cover -func=$(COVER_OUT) | tail -1
	$(GO) tool cover -html=$(COVER_OUT) -o $(COVER_HTML)

.PHONY: bench
bench: ## Run the WAL append benchmarks
	$(GO) test -run '^$$' -bench . -benchmem ./internal/storage/wal/

## ---------------------------------------------------------------- e2e

.PHONY: e2e
e2e: build ## Run the Mimir end-to-end test (needs Docker)
	./test/mimir-e2e.sh

.PHONY: ci
ci: fmt-check vet tidy-check race ## Everything CI runs on a pull request