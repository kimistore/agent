# Kimistore High Availability — Architecture Design

Status: **design / proposal** (no code in this document is implemented)
Scope: single-region HA for the `kimistore-agent`. Supersedes the ad-hoc
roadmap discussed earlier.

---

## 1. Recorded decisions

These were chosen explicitly and drive everything below. They are the
constraints, not open questions.

| # | Decision | Consequence |
|---|---|---|
| D-1 | **RPO 0 on the write path is not required.** Bounded tail loss is acceptable **if the client can be told to re-push.** | No replication, no quorum, no warm standby. Disposable agents. |
| D-2 | **Single region.** Region loss is out of scope. | S3 conditional writes are the coordination substrate. No cross-region consistency model, no regional pinning, no async replication. |
| D-3 | **Ack latency tied to the flush interval is acceptable.** | Durability posture **D2** (see §6) ships without a hard p99 bound. Adaptive flush is an optimisation, not a correctness requirement. |
| D-4 | **A full rebalance of all groups on agent failure is acceptable.** | Group state can be **ephemeral**. No durable group state machine, no per-group ownership key. This removes the single hardest item from the earlier roadmap. |
| D-5 | **`Metadata` is answered from a routing table each agent publishes** for the partitions it owns. | Per-agent routing documents in S3 (§5.2). Cheaper than answering `Metadata` from a full ownership scan. |

### The one subtlety D-1 hides

"Convey to the client to re-push" is stronger than "tail loss is OK". It
requires **detection**. Kafka's protocol gives us:

- **Consumers can be told.** If the log end moves backwards, `Fetch` returns
  `OFFSET_OUT_OF_RANGE` and the client resets. Kimistore already does this
  correctly for retention.
- **Producers cannot be told.** No Kafka error means "the offsets I acked to
  you no longer exist." `NOT_LEADER_OR_FOLLOWER` and `REPLICA_NOT_AVAILABLE`
  mean *retry this batch*, not *re-push your last N batches*. A retry of the
  current batch does not restore earlier acked-but-lost records.

So building producer-side gap notification means inventing a mechanism the
protocol has no room for. **The cheap way to satisfy D-1 is to never make the
promise: do not ack a record that is not yet recoverable from object storage.**
That is posture D2. Under D2 there is no producer-side gap to convey, and the
gap-reporting machinery is deleted from the design.

---

## 2. Goals

- **G1** Any agent may start anywhere and recover from S3 alone. Agents are
  disposable; no durable state lives in an agent.
- **G2** One writer per (topic, partition) at a time, fenced so a stale writer
  cannot corrupt the log.
- **G3** Anything acknowledged with `acks=all` survives the loss of the agent
  that acked it (durability posture D2).
- **G4** Clients route to the agent that owns a partition, preserving the
  existing hot/cold read path and the in-process long-poll latch.
- **G5** Consumer groups coordinate correctly across agents, tolerating a full
  rebalance on membership change (D-4).
- **G6** Failure is *observable* to clients through standard Kafka errors — no
  silent inconsistency.

## 3. Non-goals (explicitly not building)

- WAL replication, quorum acks, leader election, Raft/Paxos. (D-1)
- Multi-region active-active, cross-region replication. (D-2)
- S3-as-sequencer (CAS per append). It would put an object-store round trip on
  the append hot path and destroy the ack-latency and cost profile.
- Durable consumer-group state. (D-4)
- Zero-RPO write path. (D-1)

---

## 4. Why the current code blocks this (grounded)

| Blocker | Where | Why it blocks multi-agent |
|---|---|---|
| Global singleton manifest | `manifestKey = "_meta/manifest.json"`, `SaveManifestContext` in `internal/storage/engine.go` | Every agent rewrites the whole inventory. N agents clobber each other — the corruption the writer lease prevents at bucket level, reintroduced one layer up. |
| Global singleton checkpoint | `"_meta/checkpoint.json"` (`engine.go` ~L1158/L1192) | Same, for offsets/groups/topic metadata. |
| Bucket-global writer lease | `internal/storage/lease.go`, `DefaultLeaseKey` | Only one agent may exist. Ownership must become per-partition. |
| `Metadata` lies | `handleMetadata`, `internal/protocol/handler.go` | Returns one broker and `Leader = Node 0` for every partition. There is nowhere to express "this partition is on agent B", so clients can never route. |
| Coordinator is a process global | `var GlobalCoordinator = coordinator.NewCoordinator()` | Each agent coordinates every group; two agents will produce conflicting assignments (split brain). `FindCoordinator` returns self. |
| Long-poll is per-process | append latch / `signalData` | If producer and consumer are on different agents, the consumer's latch never fires and it degrades to per-fetch S3 reads. **This is why ownership is load-bearing for latency, not just safety.** |
| Tail is only in the local WAL | `MaxSegmentSize = 64MB`, size-triggered `roll()` in `internal/storage/wal/partition.go` | Crash loses up to one segment per partition, unrecoverably. Fixed by D2 for acked data. |
| Flat offset keys + full-prefix `LIST` | `_offsets/<group>/<topic>/<partition>`, `rehydrateCommittedOffsets` | Startup scan grows with total stored data; must be scoped by ownership. |

---

## 5. Target architecture

Three roles are separated. Today they are fused into one binary, one process,
and one global coordinator.

```
 clients ──Metadata──▶ routing view (union of per-agent routing tables)
                            │  partition → {owner agent, epoch}
      ┌─────────────────────┴─────────────────────┐
      │  agent-A owns t/0..t/3                    │  agent-B owns t/4..t/9
      │  local WAL (hot) + flush-on-ack           │  local WAL (hot) + flush-on-ack
      │  group coordinator for HRW(g) == A        │  group coordinator for HRW(g) == B
      └─────────────────────┬─────────────────────┘
                            │
   S3 (system of record): segments · per-partition manifests
                          liveness · ownership · routing · offsets
```

### 5.1 Object layout

| Purpose | Key | Writer | Notes |
|---|---|---|---|
| Segment data | `topic/partition/<baseOffset>-e<epoch>.log` | owner | Epoch in the key so a stale writer cannot collide with the new owner. |
| Segment index | `topic/partition/<baseOffset>-e<epoch>.index` | owner | |
| Partition manifest | `_topics/<topic>/_manifest/<partition>` | owner | Replaces global manifest. Incremental; kills whole-bucket `LIST`. |
| Agent liveness | `_agents/<agent-id>` | self | `{agent, ts, expires, advertised_host, port}`. CAS-renewed, TTL/3 cadence. |
| Partition ownership | `_owners/<topic>/<partition>` | claimant | `{agent, epoch, expires}`. CAS claim; written rarely. |
| Routing table | `_agents/<agent-id>/routing` | self | `{epoch, partitions:[{topic,partition}], updated}`. This is what `Metadata` reads (D-5). |
| Group offsets | `_offsets/<group>/<topic>/<partition>` | coordinator | Keep the key shape; add a monotonic guard (§6.3). |
| Group state | — | — | **Not persisted.** In-memory per coordinator (D-4). |

Notes:

- **Ownership is a separate, per-partition CAS claim**, not part of the agent
  lease. Contention lands on *ownership changes*, never on the append path.
- **Epoch is per-partition and monotonic.** It is carried in the segment key
  and in the routing table so clients and manifests agree on the generation.
- The writer lease from the previous change is retained but reinterpreted:
  it becomes the **agent identity/liveness lease** (`_agents/<id>`). Per-bucket
  exclusivity is dropped.

### 5.2 Routing and `Metadata` (D-5)

Each agent publishes `_agents/<id>/routing` listing exactly the partitions it
owns, with their epochs. Every agent caches the union of all routing tables
(TTL-bounded) and serves `Metadata` from it:

- Brokers = all live agents (from `_agents/` liveness + advertised host/port).
- Per partition: `Leader` = owner's node id, `LeaderEpoch` = ownership epoch.
- A partition with no live owner: `Leader = -1`, `PartitionErrorCode =
  LEADER_NOT_AVAILABLE (5)`.

This requires `Metadata` v7+ to emit `LeaderEpoch`. If the ceiling stays at v6,
routing still works with `Leader` alone; clamp and document. Today's
`handleMetadata` hardcodes broker count 1 / node 0 and must be rewritten around
the routing view.

### 5.3 Long-poll consequence

Because `Metadata` routes clients to the partition owner, Fetch reaches the
agent that also serves the append latch, so long-poll keeps working. Serving
reads for a partition from a *non-owner* is only the cold/catch-up path (direct
S3), never the steady state. This is the key reason to route by ownership rather
than letting any agent serve any partition.

---

## 6. Durability: posture D2 (ack-behind-flush)

### 6.1 Mapping to Kafka acks (faithful to Kafka semantics)

| `acks` | Meaning | Posture |
|---|---|---|
| `0` | fire and forget, no promise | unchanged |
| `1` | leader has it locally | **D1** — local fsync, current behaviour |
| `all` | strongest available | **D2** — wait until the record is in S3 and the manifest is updated |

This is a behaviour change: today `acks=all` is treated identically to
`acks=1`. Mapping `all` to D2 restores Kafka's intended meaning and lets
latency-sensitive producers opt into D1 knowingly.

### 6.2 Mechanism

1. `Append` assigns offsets and fsyncs locally as today; it also returns the
   end offset.
2. Each partition tracks `durableOffset` — advanced only after the segment
   containing the range has been uploaded to S3 **and** the partition manifest
   has been updated.
3. If `acks>=all` (D2), the produce handler waits on a per-partition
   notification until `durableOffset >= endOffset`, bounded by a timeout.
4. An **adaptive flush**: if a waiter has been pending longer than
   `flush_deadline_ms` (D-3 makes this optional), force a roll of the active
   segment regardless of size. `PartitionWAL.roll()` already supports an
   explicit roll of a small non-empty segment, so the primitive exists.
5. `acks=1` skips the wait; `acks=0` never had one.

Consequences to accept:

- Ack latency equals the flush cadence under steady state.
- A slow S3 stalls acks for that partition (head-of-line). The adaptive flush
  bounds it; there is no cross-partition impact.
- Real input for "flush interval" tuning: `MaxSegmentSize` (64MB) plus a
  time-based roll interval.

### 6.3 The residual `acks=0` / `acks=1` tail

A consumer can read past the flush point (data written with `acks<all`) and
commit an offset a restarted owner no longer has. On restart it receives
`OFFSET_OUT_OF_RANGE` and re-reads — correct under `auto.offset.reset=earliest`,
skips under `latest`. This is the one place where "convey and re-push" is
genuinely the client's policy, and it only ever affects data that was never
promised.

Offset commits must never *regress below* a previously durable commit; add a
monotonic guard on the read-modify-write of `_offsets/...`.

---

## 7. Idempotent producer (now a prerequisite, not a nice-to-have)

D2 stalls acks; stalled producers hit `delivery.timeout` and retry into a log
that already appended the batch. Without dedup you trade silent loss for
**duplicates**, which is worse for Mimir-style ingest.

- Implement `InitProducerId` (API key 22) and per-producer sequence state.
- Fencing: `producer_id` + `producer_epoch`; reject stale epochs with
  `FENCED_INSTANCE_ID (78)` / `INVALID_PRODUCER_EPOCH`.
- Convenient fact: the agent stores record batches **verbatim**, so producer
  id, epoch and sequence numbers already survive in the log. Dedup is a
  control-plane addition (last sequence per producer per partition), not a
  format change.

D2 and idempotence ship as one unit even if they land in separate commits.

---

## 8. Group coordination (D-4)

D-4 lets us delete durable group state. Design:

- **Coordinator selection = rendezvous (HRW) hashing** of `group.id` over the
  live agent set from `_agents/`. `coordinator(g) = argmax_a hash(g, a)`.
- `FindCoordinator` answers from the locally cached live set. It is a **hint**.
- The serving agent **verifies before accepting**: it must currently own a
  valid, unexpired liveness lease and the live set it uses must be current.
  Otherwise it returns `NOT_COORDINATOR (16)`.
- **Fencing:** an agent that cannot renew its `_agents/<id>` lease must
  immediately stop accepting group operations, including during the TTL grace
  window. This is the same discipline as the writer lease.
- Group state (`Group`, `GenerationID`, members, assignment) stays in memory —
  i.e. do not persist `CoordinatorState` any more. On coordinator change the
  new agent starts at generation 0; members holding an old generation get
  `ILLEGAL_GENERATION (22)` / `UNKNOWN_MEMBER_ID (25)` and rejoin. That is the
  accepted full rebalance.

Why HRW rather than modulo: when an agent leaves, only the groups it owned
remap; groups on surviving agents are untouched. This is *better* than the
accepted requirement (D-4 allows a global rebalance) and costs nothing.

Offsets remain durable in S3 and are read by the new coordinator on rebalance,
so consumers do not lose committed position across a coordinator change —
only in-flight assignments are reshuffled.

---

## 9. Failure modes and what clients observe

| Failure | Data at risk | Consumer sees | Producer sees |
|---|---|---|---|
| Owner crash, D2 (`acks=all`) | none acked | hangs until new owner; fetch resumes | retry acked for all succeeding barriers |
| Owner crash, D1/D2 (`acks=1`) | local tail since last roll | `OFFSET_OUT_OF_RANGE` → reset/replay | duplicates possible (idempotence fixes) |
| Owner graceful drain | none | brief hang, then same owner set changes | `NOT_LEADER_OR_FOLLOWER (6)` → retry, routed to new owner |
| Coordinator crash | group assignment only | `NOT_COORDINATOR` → rebalance | unaffected |
| Stale writer still running | none (fenced) | — | `NOT_LEADER_OR_FOLLOWER (6)` |
| Partition unowned | read from S3 if manifest exists | serves from object storage | `LEADER_NOT_AVAILABLE (5)` |

Client-visible errors to implement: `NOT_LEADER_OR_FOLLOWER (6)`,
`LEADER_NOT_AVAILABLE (5)`, `REPLICA_NOT_AVAILABLE (9)`,
`OFFSET_OUT_OF_RANGE (1)`, `NOT_COORDINATOR (16)`,
`COORDINATOR_NOT_AVAILABLE (15)`, `FENCED_LEADER_EPOCH (74)`,
`FENCED_INSTANCE_ID (78)`.

---

## 10. Phased plan

Ordered cheapest-risk-first. Each phase is independently shippable.

| Phase | Change | Buys | Risk |
|---|---|---|---|
| **0** | Per-partition manifests (`_topics/<t>/_manifest/<p>`); per-agent checkpoints | Removes global-key clobber; kills whole-bucket `LIST` on startup; unblocks multi-agent | Low — no behaviour change |
| **1** | D2 ack-behind-flush + adaptive flush; `acks=all`→D2 mapping | Satisfies D-1; no producer-side gap to convey | Medium — produce hot path, concurrency |
| **2** | Partition ownership (`_owners/...`, per-partition epoch) + epoch in segment keys | One writer per partition, fenced; safe multi-agent | Medium |
| **3** | Liveness + routing tables; rewrite `Metadata`; `NOT_LEADER_OR_FOLLOWER`; owner-scoped reads | Clients route to owners; long-poll preserved (G4) | Medium — touches every data handler |
| **4** | Graceful drain/handover (seal → upload → manifest → release) vs crash takeover | Measured RTO; most failovers cost no tail | Low–Medium |
| **5** | HRW group coordination + fencing + `NOT_COORDINATOR`; stop persisting group state | Group HA without durable state (D-4) | Medium |
| **6** | Idempotent producer + sequence dedup; offset monotonic guard | Makes D2 safe under retry; closes duplicate window | High |

> **Phase 0 is implemented.** The durable position now lives in per-partition
> manifests under `_topics/<topic>/_manifest/<partition>`, written incrementally
> (only partitions whose position moved), and the checkpoint is namespaced per
> agent at `_agents/<agentID>/checkpoint.json`. A bucket written by an older
> agent still has its bucket-global `_meta/checkpoint.json` and
> `_meta/manifest.json`; those are read once on upgrade and re-persisted in the
> new shape. New configuration: `KIMISTORE_AGENT_ID` (default: hostname).
>
> **Phase 1 is implemented.** `acks=all` no longer shares `acks=1`'s local
> fsync: the offset is withheld until the segment holding it is in object
> storage. A background flush seals a waiting partition every
> `KIMISTORE_FLUSH_INTERVAL_MS` (default 1000ms), and the uploader advances a
> per-partition durable watermark that only moves contiguously. The wait is
> bounded by the producer's timeout (capped at 30s) and returns
> `REQUEST_TIMED_OUT` on expiry rather than a false ack.
>
> **Phase 6 is implemented.** `InitProducerId` allocates an id from a persisted
> monotonic allocator, and produce batches carrying a producer id, epoch and
> sequence are validated and deduplicated: a retried batch is answered with the
> offset it already occupies rather than appended again. This closes the
> duplicate window Phase 1 opened. Recent sequence state is rebuilt from the
> local WAL tail on restart; a fresh-machine restart resets it. Transactions
> remain unimplemented. Validated against franz-go (the client Mimir uses),
> including a forced timeout-and-retry that asserts the retry is deduplicated.

Pull **6** forward to sit immediately with **1** where possible: shipping D2
without idempotence substitutes a worse failure mode for the one it removes.

### Sequencing dependency

Phase 3 is the hinge. Phases 0–2 are safe to ship one at a time because a
single agent can still own the whole bucket under the existing lease. Phase 3
is the first phase that only makes sense with more than one live agent; from
there, roll it out behind a flag and validate routing before enabling
multi-owner.

---

## 11. Operations prerequisites

- **TLS / mTLS between agents.** Required once agents talk about shared state
  and before routing is exposed. Client SASL PLAIN already exists; inter-agent
  auth does not.
- **Liveness false positives.** TTLs must tolerate GC pauses and brief S3
  slowness. Renew at TTL/3; treat renewal failure as *step down*, not *retry
  forever*.
- **S3 request budget.** More GETs (routing tables, manifests) and PUTs
  (segment flushes). Track object-store ops per request kind; the D2 flush rate
  is the dominant new cost.
- **Metrics.** Reuse the lease metrics; add `kimistore_writer_epoch{partition}`,
  `kimistore_durable_offset_lag{partition}`, `kimistore_flush_wait_seconds`,
  `kimistore_routing_age_seconds`, `kimistore_coordinator_owner`.

---

## 12. Open risks / to validate with a prototype

1. **Rendezvous coordinator agreement.** The stale-view split-brain window
   (§8) is closed only by the step-down-on-renewal-failure rule. Prove with a
   chaos test: pause agent B's renewal past TTL while B keeps serving; assert B
   returns `NOT_COORDINATOR` and no second generation is ever issued.
2. **Flush-on-wait under load.** Confirm the adaptive flush actually bounds
   tail latency when S3 is slow and many producers wait on one partition.
3. **Epoch-in-key garbage.** Stale-epoch segments become unreferenced. Decide
   whether the manifest-driven retention sweep owns their cleanup or a separate
   reaper does.
4. **`Metadata` ceiling.** Whether to raise the ceiling to v7+ for
   `LeaderEpoch`, or route on `Leader` alone until the protocol work lands.
5. **Routing table TTL vs. failover time.** Tune against the measured
   crash-takeover RTO from Phase 4.
