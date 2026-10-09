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

  # Build stamps. VERSION comes from the git tag so that a release and a local
  # build share one source of truth. A dirty tree appends -dirty, because a
  # binary built from uncommitted source is not the tagged release no matter
  # what the tag says.
  VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
  COMMIT     ?= $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
  BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
  VERSION_PKG := kimistore/internal/version
  LDFLAGS   := -s -w \
                -X '$(VERSION_PKG).Version=$(VERSION)' \
                -X '$(VERSION_PKG).Commit=$(COMMIT)' \
                -X '$(VERSION_PKG).Date=$(BUILD_DATE)'

  GO       ?= go
  GOFILES  := $(shell find . -name '*.go' -not -path './.git/*')

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

## ---------------------------------------------------------------- build

.PHONY: build
build: ## Build the agent binary
	$(GO) build -ldflags "$(LDFLAGS)" -o $(AGENT) ./cmd/agent

# Container images. KO_REPO is the broker image; the administration tool gets
# its own because ko builds one image per main, and because a running broker
# should not carry a binary that can rewrite its own ACLs.
KO_REPO        ?= ghcr.io/kimistore/agent
KO_CRED_REPO   ?= ghcr.io/kimistore/agent-credential
KO_PLATFORMS   ?= linux/arm64,linux/amd64

.PHONY: image
image: ## Build and push the broker image (and the credential image) with ko
	@# The stamp comes from the same VERSION the binary build uses, so an image
	@# and a binary of the same commit report the same version.
	KIMISTORE_BUILD_VERSION=$(VERSION) \
	KIMISTORE_BUILD_COMMIT=$(COMMIT) \
	KIMISTORE_BUILD_DATE=$(BUILD_DATE) \
	KO_DOCKER_REPO=$(KO_REPO) \
	  ko build --bare --platform=$(KO_PLATFORMS) ./cmd/agent
	KIMISTORE_BUILD_VERSION=$(VERSION) \
	KIMISTORE_BUILD_COMMIT=$(COMMIT) \
	KIMISTORE_BUILD_DATE=$(BUILD_DATE) \
	KO_DOCKER_REPO=$(KO_CRED_REPO) \
	  ko build --bare --platform=$(KO_PLATFORMS) ./cmd/credential

.PHONY: image-local
image-local: ## Build the broker image into the local docker daemon, unpushed
	KIMISTORE_BUILD_VERSION=$(VERSION) \
	KIMISTORE_BUILD_COMMIT=$(COMMIT) \
	KIMISTORE_BUILD_DATE=$(BUILD_DATE) \
	KO_DOCKER_REPO=$(KO_REPO) \
	  ko build --bare --local --platform=linux/arm64 ./cmd/agent

.PHONY: version
version: ## Print the build stamp this tree would produce
	@echo "VERSION=$(VERSION)"
	@echo "COMMIT=$(COMMIT)"
	@echo "DATE=$(BUILD_DATE)"

# The two architectures that matter: arm64 for the Raspberry Pi nodes, amd64 for
# any x86 peer. Release artefacts use the same list, so a binary built here
# matches one built by the workflow.
.PHONY: release-build
release-build: ## Build the release binaries for linux/arm64 and linux/amd64
	@mkdir -p $(BIN_DIR)
	@for arch in arm64 amd64; do \
	  echo "  building linux/$$arch"; \
	  CGO_ENABLED=0 GOOS=linux GOARCH=$$arch \
	    $(GO) build -ldflags "$(LDFLAGS)" \
	      -o $(BIN_DIR)/kimistore-$$arch ./cmd/agent || exit 1; \
	  CGO_ENABLED=0 GOOS=linux GOARCH=$$arch \
	    $(GO) build -ldflags "$(LDFLAGS)" \
	      -o $(BIN_DIR)/kimistore-credential-$$arch ./cmd/credential || exit 1; \
	done
	@cd $(BIN_DIR) && sha256sum kimistore-* > SHA256SUMS
	@echo "  checksums in $(BIN_DIR)/SHA256SUMS"

.PHONY: build-tools
build-tools: ## Build the benchmark and load-test tools
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/benchmark ./cmd/benchmark
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/load-test ./cmd/load-test

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

# e2e checks that the data is there. e2e-integrity checks that it is intact: it
# pushes a volume whose every sample value is computable, then verifies per-series
# counts and value sums, per-slice counts, and all of it again after a broker
# crash, a broker restart, and a full Mimir restart. It takes several minutes and
# needs the logprobe built for the container it verifies ListOffsets with.
.PHONY: e2e-integrity
e2e-integrity: build ## Run the volume integrity end-to-end test (needs Docker, slow)
	@case "$$(uname -m)" in \
	  arm64) arch=arm64 ;; \
	  *)     arch=amd64 ;; \
	esac; \
	echo "building the linux/$$arch logprobe for the docker check"; \
	GOOS=linux GOARCH=$$arch go build -o /tmp/kimi-logprobe ./test/logprobe
	./test/mimir-integrity-e2e.sh

.PHONY: ci
ci: fmt-check vet tidy-check race ## Everything CI runs on a pull request