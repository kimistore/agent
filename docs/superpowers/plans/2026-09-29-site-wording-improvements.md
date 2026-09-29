# Static Site Wording Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refresh copy across `site/index.html` and `site/README.md` to reflect the latest commit's durability and coordinator capabilities, update codebase metrics (10.5k LOC, 68 tests), and remove pre-1.0 framing in favor of architectural clarity.

**Architecture:** Static site files (`site/index.html`, `site/README.md`) updated with semantic HTML5, zero external build dependencies, keeping existing CSS class names and anchors for seamless styling and link preservation.

**Tech Stack:** HTML5, CSS3, Markdown, Python test server / xmllint / curl for verification.

## Global Constraints

- Preserve all existing CSS class names (`.status-cols`, `.status-h`, `.status-h--on`, `.status-h--todo`, `.stat`, etc.) to prevent styling regressions.
- Keep the `#status` section `id` intact to avoid breaking internal navigation and external links.
- Strictly adhere to verified codebase metrics: 10.5k LOC, 68 tests, 27 MB static binary, Go 1.27+.
- No placeholders (no TODO/TBD).

---

### Task 1: Update Navigation, Comparison Boundaries, and FAQ in `site/index.html`

**Files:**
- Modify: `site/index.html:38`, `site/index.html:54`, `site/index.html:357`, `site/index.html:362-374`, `site/index.html:634-643`, `site/index.html:703`

**Interfaces:**
- Consumes: Design spec at `docs/superpowers/specs/2026-09-29-site-wording-improvements-design.md`
- Produces: Updated navigation labels, modernized comparison table and scope callout, and updated FAQ answers reflecting current durability and recovery mechanics.

- [ ] **Step 1: Check existing anchors and texts in `site/index.html`**

Run: `grep -n 'href="#status"' site/index.html`
Expected: Lines 38, 54, 703 matching `<a href="#status">Status</a>`

- [ ] **Step 2: Update nav links and comparison section**

In `site/index.html`:
- Change desktop nav, mobile nav, and footer link text from `Status` to `Capabilities` while keeping `href="#status"`.
- In comparison table `#compare`, update "Best for" row to:
  `Single-region streaming, stream-to-lake ingestion, microservices, edge pipelines, dev/staging, cost-sensitive retention`
- In comparison callout `.callout--warn`, replace "Be honest about the boundaries" with "Clear architectural scope" and explain single-node design with S3 persistence.

- [ ] **Step 3: Update FAQ answers for Kafka replacement and agent failure**

In `site/index.html` under `#faq`:
- Refine "Is this a Kafka replacement?" to position Kimistore as a specialized streaming engine that eliminates cluster operational overhead and storage costs for single-region and lakehouse workloads.
- Update "What happens if the agent dies?" to detail group-commit fsync on `acks=1`, automatic torn-tail truncation, and S3-backed offset rehydration with shutdown flush retries.

- [ ] **Step 4: Verify syntax and anchors**

Run: `grep -n 'Capabilities' site/index.html`
Expected: Occurrences in desktop nav, mobile nav, and footer.

- [ ] **Step 5: Commit changes**

```bash
git add site/index.html
git commit -m "docs(site): update navigation, comparison scope, and FAQ copy"
```

---

### Task 2: Update Metrics and Capabilities Section in `site/index.html`

**Files:**
- Modify: `site/index.html:566-624`

**Interfaces:**
- Consumes: Task 1 updates and spec
- Produces: Reframed `#status` section with 10.5k LOC, 68 passing tests, built-in guarantees list, and architectural non-goals list.

- [ ] **Step 1: Inspect current `#status` section**

Run: `grep -n -C 5 'id="status"' site/index.html`

- [ ] **Step 2: Rewrite `#status` section header and stats**

In `site/index.html`:
- Eyebrow: `Engine capabilities`
- Title: `Engineered for durability and operational simplicity`
- Lede: `Built from the ground up to deliver Kafka wire-protocol compatibility with object storage economics. Every guarantee is verified by comprehensive durability, crash-recovery, and protocol test suites.`
- Stats:
  - `10.5k` lines of Go
  - `68` unit & integration tests, passing
  - `27 MB` single static binary

- [ ] **Step 3: Rewrite capabilities and architectural scope columns**

In `site/index.html`:
- Left column heading (`.status-h--on`): `Built-in guarantees & capabilities`
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
- Right column heading (`.status-h--todo`): `Architectural scope & non-goals`
  - Single-node architecture by design: no multi-broker replication, ISR quorums, or partition leader elections
  - Standard Kafka at-least-once streaming: no 2PC distributed transactions or transactional producer IDs
  - Built-in SASL PLAIN authentication: topic-level ACL authorization is not enforced
  - Dynamic consumer rebalance assignment: delegates partition assignment to group leader without cooperative sticky protocols

- [ ] **Step 4: Verify HTML tag pairing and text**

Run: `grep -n -C 10 '10.5k' site/index.html`
Expected: Shows stats block and cleanly formatted columns.

- [ ] **Step 5: Commit changes**

```bash
git add site/index.html
git commit -m "docs(site): reframe capabilities and update metrics to 10.5k LOC and 68 tests"
```

---

### Task 3: Update `site/README.md` Documentation and Guidelines

**Files:**
- Modify: `site/README.md:46-47`, `site/README.md:67-69`

**Interfaces:**
- Consumes: Updated `site/index.html`
- Produces: Synchronized documentation in `site/README.md`.

- [ ] **Step 1: Check existing metrics and tone notes in `site/README.md`**

Run: `grep -n -C 3 'Check the numbers' site/README.md`

- [ ] **Step 2: Update metrics and tone instructions**

In `site/README.md`:
- Update item 4 under "Before you publish":
  - Change `"6.6k lines of Go", "12 tests"` to `"10.5k lines of Go", "68 tests"`.
- Update "Tone" section:
  - Replace "Kimistore is pre-1.0 and single-node" with guidance focusing on transparent architectural boundaries (single-node S3 streaming vs distributed KRaft clusters) without artificial "pre-1.0" apologies.

- [ ] **Step 3: Verify markdown formatting**

Run: `git diff site/README.md`
Expected: Clean diff reflecting new metrics and tone guidance.

- [ ] **Step 4: Commit changes**

```bash
git add site/README.md
git commit -m "docs(site): update numbers and tone guide in site/README.md"
```

---

### Task 4: Full Site Verification

**Files:**
- Read-only check: `site/index.html`, `site/README.md`

**Interfaces:**
- Consumes: Completed edits from Tasks 1-3
- Produces: Verified static site without broken tags, missing anchors, or regression in styling.

- [ ] **Step 1: Verify all anchor references exist**

Run: python script or command to extract all `<a href="#...">` in `site/index.html` and check that corresponding `id="..."` elements exist.
Expected: Every `#id` has a matching element in `site/index.html`.

- [ ] **Step 2: Check for any remaining 'pre-1.0' or 'early, honest' phrases**

Run: `grep -inE "pre-1|early, honest|not there yet" site/*`
Expected: No occurrences found.

- [ ] **Step 3: Verify git status and log**

Run: `git status` and `git log -n 4 --oneline`
Expected: Clean working tree, clear commit messages.
