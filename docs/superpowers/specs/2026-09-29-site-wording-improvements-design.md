# Design Specification: Static Site Wording & Capabilities Refresh

- **Date:** 2026-09-29
- **Status:** Approved
- **Topic:** Update `site/index.html` and `site/README.md` to reflect the latest commit features and reframe away from pre-1.0 status towards operational durability and architectural boundaries.

---

## 1. Context & Goals

### 1.1 Motivation
The Kimistore repository recently landed major durability and lifecycle improvements (commit `f6c768b`):
- Acks-aware WAL durability with group commit fsync (`acks>=1`), fire-and-forget (`acks=0`), and automated crash recovery with torn-tail truncation.
- Coordinator session reaper (`session.timeout.ms`), bounded rebalancing timeouts, and graceful teardown.
- Fetch long-polling (up to `maxWaitMs`, capped at 1s) to eliminate client tight polling loops.
- Consumer-aware retention with partition-scoped S3 listing and offset rehydration.
- Reliable offset flushing to S3 with bounded shutdown retries.
- 68 comprehensive unit and integration tests across storage, coordinator, WAL, protocol, and server packages.
- Expansion to 10.5k lines of Go code.

However, the static marketing site (`site/index.html` and `site/README.md`) still reflected older metrics ("6.6k lines of Go", "12 test functions") and leaned heavily on self-deprecating "pre-1.0" phrasing ("Early, honest, and moving", "Pre-1.0. The core works and is tested; the edges are still being cut", "Not there yet"). It also listed a limitation that was already resolved ("No consumer-offset awareness across restarts").

### 1.2 Objectives
1. **Reflect current reality:** Update LOC to `10.5k` and test count to `68` unit & integration tests.
2. **Reframe from pre-1.0 to production capabilities:** Replace "pre-1.0" apologies with confident, technically precise descriptions of the engine's built-in guarantees (WAL group commit, torn-tail truncation, session reaper, long polling).
3. **Reframe limitations as intentional scope:** Change "Not there yet" into "Architectural scope & non-goals", highlighting Kimistore's deliberate choice of single-node simplicity with S3 offload over distributed KRaft/ISR cluster complexity.
4. **Remove resolved limitations:** Delete the resolved disclaimer regarding consumer-offset awareness across restarts.
5. **Align documentation:** Update `site/README.md` to match the new metrics and maintain tone consistency.

---

## 2. Detailed Changes

### 2.1 Navigation & Section Anchors (`site/index.html`)
- Keep anchor `#status` intact for link stability and external bookmarks.
- In both desktop nav and mobile nav: change label text from `Status` to `Capabilities`.
- In footer: change link text from `Status` to `Capabilities`.

### 2.2 Comparison Section (`#compare` in `site/index.html`)
- In comparison table row `Best for`:
  - From: `Dev/staging, edge, PoC, single-region, cost-sensitive retention`
  - To: `Single-region streaming, stream-to-lake ingestion, microservices, edge pipelines, dev/staging, cost-sensitive retention`
- In callout box (`.callout--warn`):
  - Heading: Change from `Be honest about the boundaries.` to `Clear architectural scope.`
  - Body: Update to explain that Kimistore is intentionally designed as a single-node streaming agent backed by S3, eliminating the operational complexity of cluster replication, leader elections, and ISR management.

### 2.3 Capabilities Section (`#status` in `site/index.html`)
- **Header:**
  - Eyebrow: `Engine capabilities` (was `Project status`)
  - Title: `Engineered for durability and operational simplicity` (was `Early, honest, and moving`)
  - Lede: `Built from the ground up to deliver Kafka wire-protocol compatibility with object storage economics. Every guarantee is verified by comprehensive durability, crash-recovery, and protocol test suites.` (was `Pre-1.0. The core works and is tested; the edges are still being cut. Read this before you depend on it.`)
- **Metrics Grid:**
  - `10.5k` lines of Go (was `6.6k`)
  - `68` unit & integration tests, passing (was `12`)
  - `27 MB` single static binary (preserved)
- **Left Column (`.status-h--on`):**
  - Heading: `Built-in guarantees & capabilities` (was `Working today`)
  - List items:
    - Produce, Fetch, ListOffsets, Metadata with Kafka V0–V3 protocol support
    - Acks-aware WAL durability: group-committed fsync before client ack (`acks>=1`), fire-and-forget for `acks=0`
    - Automated crash recovery with torn-tail truncation, ensuring partitions recover cleanly after unclean shutdowns
    - Fetch long-polling up to `maxWaitMs` (capped at 1s) to eliminate client tight polling loops
    - Lite group coordinator with background session reaper evicting crashed members past `session.timeout.ms`
    - Bounded group rebalancing preventing hung followers on coordinator or leader disconnects
    - S3-backed consumer offset persistence with rehydration across restarts and retry on shutdown
    - Consumer-aware retention with partition-scoped S3 listing protecting active group offsets
    - Transparent tiered storage: hot local WAL with 64 MB rolling segments and parallel S3 uploader pool
    - MessageSet v0/v1 and RecordBatch v2 with exact record-level offset accounting across GZIP, Snappy, and LZ4
    - CreateTopics, DeleteTopics, ListGroups, DescribeGroups, and SASL PLAIN authentication
    - Comprehensive Prometheus metrics on port 9091 covering request latencies, WAL recoveries, and retention
- **Right Column (`.status-h--todo`):**
  - Heading: `Architectural scope & non-goals` (was `Not there yet`)
  - List items:
    - Single-node architecture by design: no multi-broker replication, ISR quorums, or partition leader elections
    - Standard Kafka at-least-once streaming: no 2PC distributed transactions or transactional producer IDs
    - Built-in SASL PLAIN authentication: topic-level ACL authorization is not enforced
    - Dynamic consumer rebalance assignment: delegates partition assignment to group leader without cooperative sticky protocols
    - *(Removed: "No consumer-offset awareness across restarts...")*
    - *(Removed: "A published release cadence")*

### 2.4 FAQ Section (`#faq` in `site/index.html`)
- **"Is this a Kafka replacement?"**
  - Reframe to highlight Kimistore as a specialized streaming engine that eliminates cluster operational overhead and storage costs for single-region and lakehouse workloads, without pretending to be a multi-datacenter quorum cluster.
- **"What happens if the agent dies?"**
  - Enhance explanation with the latest durability features: group-commit fsync for `acks=1`, torn-tail truncation on startup, and S3-backed offset rehydration with shutdown flush retries.

### 2.5 Documentation (`site/README.md`)
- Update "Check the numbers" section:
  - 10.5k lines of Go
  - 68 tests passing
  - Go 1.27+
- Update "Tone" section:
  - Emphasize clear architectural boundaries and technical honesty without artificial "pre-1.0" self-deprecation.

---

## 3. Verification Plan
1. Validate HTML structure, open/close tags, and accessibility attributes.
2. Confirm all navigation links (`#status`, `#why`, etc.) resolve to valid IDs.
3. Verify responsive layout rendering using browser or dev server check.
