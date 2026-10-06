# Kimistore High Availability — Architecture Design

Status: **all six phases implemented** and on `main`. §13 is the handoff: what
landed, the two silent bugs a volume test found, and §14 what is still missing.
`mimir-integrity-e2e.sh` verifies the result under load; see §13.7.
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
| Bucket-global writer lease | `internal/storage/lease.go`, `DefaultLeaseKey` | Only one agent may exist. **Resolved by phase 2**: the claim is now per partition. |
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
| Agent liveness | `_agents/<agent-id>/liveness` | self | `{agent, ts, expires, advertised_host, port}`. CAS-renewed, TTL/3 cadence. |
| Partition ownership | `_owners/<topic>/<partition>` | claimant | `{agent, epoch, expires}`. CAS claim; written rarely. |
| Routing table | `_agents/<agent-id>/routing` | self | `{epoch, partitions:[{topic,partition}], updated}`. This is what `Metadata` reads (D-5). |
| Group offsets | `_offsets/<group>/<topic>/<partition>` | coordinator | Keep the key shape; add a monotonic guard (§6.3). |
| Group state | — | — | **Not persisted.** In-memory per coordinator (D-4). |

Notes:

- **Ownership is a separate, per-partition CAS claim**, not part of the agent
  lease. Contention lands on *ownership changes*, never on the append path.
- **Epoch is per-partition and monotonic.** It is carried in the segment key
  and in the routing table so clients and manifests agree on the generation.
- The bucket-global writer lease is **the fallback fence**, not a second one. It
  is only acquired when `KIMISTORE_PARTITION_OWNERSHIP=false`, in which case it is
  the whole fence and per-bucket exclusivity is back. With ownership on (the
  default) it is not acquired at all. Phase 3 adds the **agent identity/liveness
  lease** (`_agents/<id>/liveness`), which is a different thing: it is what tells clients
  the agent is alive, not what fences the log.

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
- **Fencing:** an agent that cannot renew its `_agents/<id>/liveness` lease must
  immediately stop accepting group operations, including during the TTL grace
  window. This is the same discipline as partition ownership.
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
| **2** | Partition ownership (`_owners/...`, per-partition epoch) + epoch in segment keys | One writer per partition, fenced; safe multi-agent | ✅ done |
| **3** | Liveness + routing tables; rewrite `Metadata`; owner-scoped reads | ✅ done |
| **3b** | HRW group coordination | Groups survive an agent moving | Medium — touches the coordinator |
| **4** | Graceful drain/handover (seal → upload → manifest → release) vs crash takeover | Measured RTO; most failovers cost no tail | ⬜ next |
| **5** | HRW group coordination + fencing + `NOT_COORDINATOR`; stop persisting group state | ✅ done |
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

Phase **6 was pulled forward** to land immediately with **1**: shipping D2
without idempotence substitutes a worse failure mode for the one it removes,
so the two are done together (commits `67bafd0` and `702b28a`).

> **Phase 2 is implemented.** Each `(topic, partition)` is claimed separately at
> `_owners/<topic>/<partition>` with the same expiring-record
> compare-and-swap the writer lease used, so more than one agent can serve one
> bucket. The per-partition epoch goes into the segment key
> (`<baseOffset>-e<epoch>.log`) and into the partition manifest. A produce for a
> partition this agent does not hold is refused with
> `NOT_LEADER_OR_FOLLOWER`, and the manifest for an unowned partition is never
> written. The bucket-global writer lease is now the *fallback* fence: it is only
> acquired when `KIMISTORE_PARTITION_OWNERSHIP=false`. New settings:
> `KIMISTORE_PARTITION_OWNERSHIP`, `KIMISTORE_OWNERSHIP_TTL_MS`.
>
> Two consequences worth naming, because they are deliberate and visible:
> ownership replaced the lease rather than sitting on top of it (one fence at a
> time, or there would be two epochs per write and only one of them would fence
> anything), and a claim that cannot be renewed is **not** re-acquired in the same
> process — the epoch would be newer but the agent's in-memory log position would
> not, so writing would reissue offsets that are already taken. The partition
> stays fenced until a restart recovers it.

> **Phase 3 is implemented.** Each agent publishes two objects under its own
> `_agents/<id>/` namespace: a CAS-renewed **liveness** record (`_agents/<id>/liveness`, carrying
> the node id and advertised host/port) and a **routing table**
> (`_agents/<id>/routing`, carrying the partitions it owns with their ownership
> epochs). Every agent caches the union of all live agents' tables, and `Metadata`
> answers from it: brokers are the live agents, each partition's `Leader` is the
> node id of its owner and its `LeaderEpoch` the ownership epoch. A partition whose
> owner is missing or whose table has gone stale is reported with
> `LEADER_NOT_AVAILABLE` and `Leader = -1`, which is what makes a client refresh
> and retry rather than give up. `Fetch` and `ListOffsets` refuse a partition this
> agent does not own, and a fetch never parks on one. New setting:
> `KIMISTORE_NODE_ID` (derived from the agent id when unset).
>
> Three decisions in here are worth naming, because each one was a real bug before
> it was a decision:
>
> - **The topic/partition inventory comes from the `_topics/` manifest keys, not
>   from routing tables.** A partition nobody owns appears in no routing table, and
>   Metadata still has to report it — otherwise a client that is not told the
>   partition exists will hash keys onto it and go nowhere. This is what makes the
>   topic/partition set cluster-wide rather than "whatever I own", which is what a
>   client needs to route at all.
> - **A newly claimed partition is advertised immediately**, not on the next
>   refresh tick. Waiting would mean a client asking Metadata seconds after a topic
>   was created is told it has no leader, for up to a whole interval.
> - **A failed refresh keeps the previous view and marks it stale.** Reporting an
>   empty cluster because one LIST timed out would take every client's metadata away
>   at once. A stale view costs one extra Metadata round trip; an empty one costs
>   the cluster.
>
> **Phase 5 is implemented.** Coordinator selection is rendezvous (highest random
> weight) hashing of the group id over the live agent set, so it is a pure
> function two agents with the same view compute identically, and when an agent
> leaves only the groups it coordinated move. `FindCoordinator` answers with that
> agent's node id and address; the client treats it as a hint. **Every** group
> request -- `JoinGroup`, `SyncGroup`, `Heartbeat` -- re-checks against this
> agent's own live set and answers `NOT_COORDINATOR` (16) if this agent is not the
> winner. That check at the point of use is the fence: it is what makes two
> coordinators for one group unreachable while their views differ, and it is why
> the selection being deterministic matters more than the selection being right.
>
> Two decisions are deliberate and worth stating:
>
> - **There is no grace window on the liveness fence.** An agent that fails to
>   renew its liveness record stops coordinating *immediately*, where partition
>   ownership waits out the TTL. A partition owner that loses its claim is still
>   fenced by the epoch in every segment name and manifest; a coordinator has no
>   such token, so it may already have been replaced. The cost is a rebalance on a
>   transient object-store blip, which is the cheaper of the two failures.
> - **`LeaveGroup` is not fenced.** A member must be able to tell *someone* it is
>   leaving, and the agent it thought it was talking to is the one most likely to
>   have changed. The reaper converges the group regardless, so dropping a leave
>   that arrived at the wrong agent costs nothing beyond the member's own session
>   timeout.
>
> Group state is no longer persisted. It used to be replayed from the checkpoint,
> which produced a group that looked alive but whose restored members had
> connections belonging to a process that had exited; the reaper eventually
> evicted them, so the only effect was a window of wrong assignments. A
> coordinator now starts empty and members rejoin, which is the same full rebalance
> D-4 accepts. Consumer offsets are unaffected: they were always durable in object
> storage, and that is what makes the rejoin cheap.

> **`Metadata` is now advertised up to v7**, which settles open risk 4: v7 is the
> newest non-flexible version and the only field it adds is `LeaderEpoch`. v8 adds
> topic authorization, which is not implemented, so v7 is the ceiling.

### Sequencing dependency

Phases 0–2 were safe to ship one at a time because a single agent could still own
the whole bucket. Phase 3 is the first that only makes sense with more than one
live agent, so it should be rolled out by starting one agent, watching its
advertised broker entry and its routing table in object storage, and only then
adding a second.

The routing TTL is the knob that decides how quickly a dead agent stops being
advertised, and it is deliberately the same value as the ownership TTL: a client
should never be sent to an agent that has lost the right to serve the partition,
and the two must not disagree about when that happened.

---

## 11. Operations prerequisites

- **TLS / mTLS between agents.** Required once agents talk about shared state
  and before routing is exposed. Client SASL PLAIN already exists; inter-agent
  auth does not. Note that with phase 3 the advertised address is now load-bearing
  in a way it was not before: a client will be *redirected* to it, so a wrong
  `KIMISTORE_ADVERTISED_HOST` moves clients off a working broker onto a broken
  one.
- **Liveness false positives.** TTLs must tolerate GC pauses and brief S3
  slowness. Renew at TTL/3; treat renewal failure as *step down*, not *retry
  forever*. This now has a second consequence worth stating plainly: a live agent
  whose routing table has gone stale is reported as owning nothing, so a
  `kimistore_routing_age_seconds` that stops climbing while an agent is up means
  clients are being sent nowhere.
- **S3 request budget.** More GETs (routing tables, manifests) and PUTs
  (segment flushes). Track object-store ops per request kind; the D2 flush rate
  is the dominant new cost.
- **Metrics.** Phase 2 shipped `kimistore_partitions_owned`,
  `kimistore_writer_epoch{partition}`, `kimistore_ownership_claim_failures_total`,
  `kimistore_ownership_renewal_failures_total` and
  `kimistore_ownership_refused_writes_total`. Phase 3 added
  `kimistore_agents_live`, `kimistore_routing_brokers`,
  `kimistore_routing_age_seconds`, `kimistore_routing_publish_conflicts_total`,
  `kimistore_routing_inconsistent_tables_total`,
  `kimistore_routing_duplicate_node_ids_total` and
  `kimistore_routing_inventory_failures_total`. Still to come:
  `kimistore_durable_offset_lag{partition}`, `kimistore_coordinator_owner`.
- **Ownership claim fan-out.** A claim is renewed every `TTL/3`, so an agent
  holding N partitions issues N PUTs per interval against `_owners/`. At 60s TTL
  and, say, 500 partitions that is ~8 PUTs/second of pure overhead. If that
  matters, the fix is to renew claims only for partitions that have seen a write
  recently, and to let a quiet partition's claim lapse — which is safe precisely
  because a lapsed claim only means somebody else *may* take it, and nothing is
  being written. Measure before adding the complexity.

---

## 12. Open risks / to validate with a prototype

1. ~~**Rendezvous coordinator agreement.**~~ **Partly closed in phase 5.** The
   step-down-on-renewal-failure rule is implemented with no grace window, and the
   point-of-use fence is covered by tests that run two agents against one bucket.
   The window that remains is the refresh interval: two agents can hold different
   live sets for up to TTL/3 and both believe they are the coordinator. The fence
   bounds the damage to one rebalance rather than a split brain, but proving it
   wants a chaos test: pause agent B's renewal past TTL while B keeps serving, and
   assert that every group B held is refused and that no second generation is
   issued while B is still up.
2. **Flush-on-wait under load.** Confirm the adaptive flush actually bounds
   tail latency when S3 is slow and many producers wait on one partition.
3. ~~**Epoch-in-key garbage.**~~ **Settled in phase 2:** the retention sweep
   owns the cleanup. Superseded-epoch segments are unreachable, so it deletes
   them unconditionally rather than waiting for a consumer to advance past them,
   and runs its own policies over the live segments only.
4. ~~**`Metadata` ceiling.**~~ **Settled in phase 3:** the ceiling is v7, the
   newest non-flexible version and the only field past v6 being `LeaderEpoch`.
   v8's topic authorization is not implemented, so v7 is where it stops.
5. **Routing table TTL vs. failover time.** Tune against the measured
   crash-takeover RTO from Phase 4. Note the TTL has two jobs — how long a dead
   agent stays advertised, and how long a wedged one does — and it is currently
   the same knob for both.
6. **Discovery cost.** A refresh is one `LIST _agents/` plus one `LIST _topics/`
   plus a `GET` per agent. At the 60s default with a handful of agents that is
   negligible; at a thousand agents it is not, and the obvious fix (cache each
   peer's table and poll them round-robin rather than re-listing every time) is
   not built. Measure before adding it.

---

## 13. Implementation handoff

Written so the next session can start without re-deriving any of this.

### 13.1 Where things stand

| Phase | State | Commit | New surface |
|---|---|---|---|
| 0 — per-partition manifests, per-agent checkpoints | ✅ on `main` | `a9127c8` | `_topics/<t>/_manifest/<p>`, `_agents/<id>/checkpoint.json`, `KIMISTORE_AGENT_ID` |
| 1 — `acks=all` waits for object storage (D2) | ✅ on `main` | `67bafd0` | `KIMISTORE_FLUSH_INTERVAL_MS`, durable watermark, `REQUEST_TIMED_OUT` |
| 6 — idempotent producer + sequence dedup | ✅ on `main` | `702b28a` | `InitProducerId` (22), producer state, `_producers/_seq` |
| 2 — per-partition ownership + epoch in segment keys | ✅ on `main` | — | `_owners/<t>/<p>`, `KIMISTORE_PARTITION_OWNERSHIP`, `NOT_LEADER_OR_FOLLOWER` |
| 3 — liveness + routing tables, leader-aware `Metadata` | ✅ on `main` | — | `_agents/<id>/liveness`, `_agents/<id>/routing`, `KIMISTORE_NODE_ID`, Metadata v7 |
| 5 — HRW group coordination | ✅ on `main` | — | `NOT_COORDINATOR`, in-memory-only group state |
| 4 — graceful drain/handover vs crash takeover | ✅ on `main` | — | `DrainPartition`, recovery reconciliation, handover metrics |

Phases 0/1/6 make a single-writer agent correct under failure, phase 2 makes
several agents safe on one bucket, and phase 3 makes them *usable*: `Metadata` now
reports every live agent and names the owner of each partition, so a client routes
itself to whoever holds what it wants to read and write.

Phase 5 made groups HA as well: `FindCoordinator` answers by rendezvous hashing
over the live set, and every group request is fenced, so an agent that is not the
coordinator answers `NOT_COORDINATOR` instead of acting. Group state is in-memory
only, so a coordinator change is a full rebalance — which is what D-4 accepted.

Phase 4 closed the loop: a partition can now be handed to another agent while this
one keeps serving, and — more importantly — recovery no longer trusts a manifest
that a crash may have left behind the objects it describes, which was silently
overwriting acknowledged records on takeover. All six phases are now implemented;
§13.8 is what remains.

### 13.2 Key files

| Concern | File |
|---|---|
| Durable position, checkpoint, write path, upload | `internal/storage/engine.go` |
| Per-partition manifests + legacy migration | `internal/storage/partition_manifest.go` |
| **Per-partition ownership claims, renewal, release** | `internal/storage/ownership.go` |
| **Segment names carrying the ownership epoch** | `internal/storage/wal/segment_name.go` |
| **Liveness, routing tables, cached cluster view** | `internal/storage/registry.go` |
| **Rendezvous coordinator selection** | `internal/coordinator/rendezvous.go` |
| Bucket-global writer lease (fallback fence) | `internal/storage/lease.go` |
| D2 durable watermark + flush loop | `internal/storage/durable.go` |
| Idempotent producer state + allocator + WAL-tail recovery | `internal/storage/producer.go` |
| RecordBatch parsing (incl. producer header) | `internal/storage/wal/record.go` |
| Segment roll, explicit flush, upload task | `internal/storage/wal/partition.go`, `manager.go` |
| Protocol dispatch, API versions, error codes | `internal/protocol/handler.go` |
| Produce + InitProducerId handlers | `internal/protocol/handlers_ops.go` |
| Config | `internal/config/config.go` |
| Metrics | `internal/metrics/metrics.go` |

### 13.3 Phase 2 — what landed

Shipped. One writer per **(topic, partition)**, fenced, so more than one agent
can serve one bucket. `internal/storage/ownership.go`:

1. A per-partition claim at `_owners/<topic>/<partition>` =
   `{agent, epoch, expires}`, acquired with the same CAS helper the lease uses
   (`ConditionalObjectStore.PutVersion`). Claims are taken lazily, once per
   partition per process, so the CAS is off the append hot path.
2. The ownership epoch is in the segment key, via `wal.SegmentName`
   (`internal/storage/wal/segment_name.go`). Both the local sealed file name and
   the object key carry it, so a stale owner's upload lands beside its
   successor's rather than over it, and a reconciliation re-upload reproduces the
   key it already had.
3. The epoch is **monotonic per partition** and never reset, for the same reason
   the lease epoch is not (see the `release` tombstone in `lease.go`): it is
   stamped into the manifest and the segment names, and a reset would make the
   next agent conclude it had been superseded by a writer that does not exist.
4. `loadPartitionManifests` claims a partition before reading its manifest (the
   LIST may name partitions the checkpoint did not know), and compares the
   manifest's epoch against the claim, returning `errSuperseded` on a mismatch.
5. `produceError` maps `ErrPartitionHeld` / `ErrPartitionNotOwned` /
   `ErrPartitionLost` to `NOT_LEADER_OR_FOLLOWER`, which is what sends a producer
   to the right agent.

### 13.4 Phase 3 — shipped, and what it left

`internal/storage/registry.go`. Liveness and routing records, the cached union
view, and the discovery loop that keeps them current. `handleMetadata` is
rewritten around that view.

One design decision deserves its own note, because the obvious alternative is
wrong: **a fetch for a partition this agent does not own is refused, not served
cold from object storage.** §5.3 assumed the non-owner read path was "cold S3
reads". That turns out not to be available: a non-owner does not know the
partition's log end offset, because its durable position was dropped when the
claim moved. Every high watermark it could report would be a guess, and a
low guess makes a consumer believe it is caught up and stop reading, while a high
guess makes it skip records it never read. `LEADER_NOT_AVAILABLE` costs one
Metadata round trip and gets the consumer to the agent that knows the answer. The
same reasoning applies to `ListOffsets`, where reporting offset 0 as "earliest"
for a log that starts at 90000 is an infinite `OffsetOutOfRange` loop for the
client that trusts it.

What phase 3 leaves, and where phase 5 picked it up:

1. **Phase 5 — group coordination.** Shipped; see the phase 5 note above and §8
   for the design. `FindCoordinator` no longer returns self unconditionally.
2. **Phase 4 — handover.** Shipped; see §13.7.

### 13.5 Phase 4 — handover

Phase 4 has two halves: a correctness fix that recovery needed before any handover
could be safe, and the handover itself.

#### The recovery fix that handover depends on

An owner appends to offset 20, uploads the segment, and dies before the next
checkpoint. Its manifest still says 10. The next owner trusts it and starts
writing at 10 — over records the previous owner had already acknowledged, because
D2 releases an `acks=all` producer the moment the segment lands, not when a
manifest catches up. Nothing in the manifest hints the objects are further ahead.

No ordering removes that window: the segment has to become visible before the
manifest that describes it can be written, so a crash in between always leaves a
manifest behind its own data. **Object storage is the system of record, so
recovery derives the position from it and takes the maximum of the two.** Every
partition with segments is reconciled at startup, not just the ones whose manifest
looks incomplete; `kimistore_objects_reconciled_total` counts the corrections, and
recovery logs each one loudly, because a non-zero rate means the tail of the system
is failing more often than the dashboards suggest.

This is a deliberate trade against startup cost, which the standing rule says loses
to data loss. The cost is contained by batching: one LIST per topic rather than one
per partition, plus one bounded tail read per partition (the index sidecar, then
the last indexed record — about 4KB plus one record, never the whole segment).

It also closes a second door. A manifest from a *superseded* epoch is normally
rejected, but that check only catches a manifest from a **newer** owner; a stale
writer pushing the log end backwards was unguarded. `TestRecovery_StaleEpochManifestCannotRewindTheLog`
pins that the reconciliation is what makes it safe.

#### The handover

`DrainPartition` moves one partition while the agent keeps serving everything else.
The order is load-bearing:

1. seal the active segment and wait for the upload to land,
2. wait until the durable frontier covers the log end,
3. re-verify the claim against object storage,
4. write the partition manifest,
5. release the ownership claim, and only then stop serving the partition.

**The release is last on purpose.** The claim is gone the instant it is released,
so another agent may take the partition immediately; a handover that released first
and flushed after would let a peer take a partition this agent was still appending
to. Steps 3 and the re-check after step 2 exist because the local view of ownership
goes stale — a claim can be taken the moment it ages out, and this agent learns
only at its next renewal, up to a third of a TTL later.

**A handover that cannot make the tail durable keeps the partition** rather than
releasing anyway, and reports why. Releasing anyway is what turns a slow object
store into silent data loss. A slow handover is a slower recovery; a wrong one is
not a recovery at all. `kimistore_handover_kept_total` makes the refused case
visible rather than merely logged.

Reads are deliberately *not* fenced on ownership. A non-owner reads from object
storage, which is authoritative, and refusing reads would break consumers during
every handover for no safety gain. What must never happen is the old owner serving
its own pre-handover WAL, and that is the read-side fence from phase 2.

#### RTO is now measured, not assumed

That was the phase's actual deliverable. Every handover records
`kimistore_handover_seconds` (seal to released claim) and
`kimistore_handover_completed_total`. A clean handover costs one seal plus one
manifest PUT, so the interesting number is the crash path: a takeover waits out the
claim TTL (`KIMISTORE_OWNERSHIP_TTL_MS`, default 60s, renewed every TTL/3), and
the RTO that matters to a producer is the TTL, not the drain. A healthy cluster should show `handover_kept_total` at zero — a non-zero
value means object storage is too slow to hand anything over safely, which is a
capacity problem worth seeing before it is a data-loss problem.

#### Two bugs this found, both silent

The volume test exists to check integrity, and it found two things that nothing
else had:

**`ListOffsets("earliest") pointed past data that was still present.** Recovery
took the log start from the newest segment instead of the oldest — the entry walk
it already does only sees the newest segment — so once a partition had more than
one uploaded segment, every consumer that reset to the start of the log was sent
past everything older. Mimir's group offsets are in-memory only, so *every Mimir
restart* re-seeks to the start of the log, which is exactly why a restarted Mimir
silently lost the head of the log. Nothing errored: the records were still in
object storage and readable, and the broker reported a start offset past them.
Recovery now derives both ends from the objects, taking the maximum for the log end
and the minimum for the log start.

**The Metadata response emitted `Partition` before `ErrorCode`.** Kafka's
`MetadataResponseTopicPartition` is error code first. The swap does not break a
decode of the *first* partition, so every single-partition test passed and every
existing decoder here read the fields in the wrong order without checking their
values. It only surfaced with more than one partition, where a client checking
that partition numbers are consecutive reads the second one as 65536 — which is
what Grafana Mimir reported. `TestMetadataDecodesWithARealKafkaClient` now parses
the response with franz-go's decoder, the one Mimir actually uses, and asserts the
partition numbers; a decoder that shares the mistake cannot catch it.

#### What phase 4 does not do

There is no runtime trigger. `DrainPartition` is an engine API with tests but no
caller: exposing it needs an authenticated mutating endpoint or an operator
protocol, and inventing either is a decision about the trust boundary rather than
about the storage engine. Note in particular that **deleting the claim out of band
is not a substitute** — it releases the partition immediately while the old owner
keeps serving until its next renewal, which is precisely the split-brain window
the epoch in the segment key mitigates but does not close.

### 13.6 Decisions made during implementation (refinements to this doc)

- **`KIMISTORE_AGENT_ID` defaults to the hostname, not the lease holder.** The
  checkpoint key must be stable across a restart; the lease holder is
  `hostname/pid` on purpose because it needs per-process uniqueness. Two
  identities, two jobs.
- **D2 fragments small synchronous writes.** A producer that writes one record
  and waits cannot coalesce, so each ack seals its own segment object. Batched
  or concurrent producers amortise this; the default 1s flush interval means a
  low-rate synchronous producer costs ~1 PUT/s/partition. Accepted, not a bug.
- **Producer state is recovered from the local WAL tail only.** A same-machine
  restart dedups; a fresh-machine restart resets producer state and an
  idempotent retry can duplicate. Documented in README §2f; Mimir tolerates it.
- **The "offset monotonic guard" from phase 6 was read as:** a deduplicated
  batch is answered with its original offset (never a fresh one), and
  `MetadataCache.AdvancePartition` only ever moves the log end forward. If a
  stronger guarantee is wanted (reject a *lower* consumer commit), that is not
  implemented — Kafka permits lowering commits, so it was left conformant.
- **Ownership replaced the bucket-global lease rather than joining it.** There is
  one fence at a time. Two overlapping claims would mean two epochs per write and
  only one of them fencing anything, and the coarse one would refuse every second
  agent — the exact behaviour per-partition ownership exists to remove. The lease
  is now what you get from `KIMISTORE_PARTITION_OWNERSHIP=false`.
- **A partition another agent holds is skipped, not fatal.** The alternative was
  refusing to start, which leaves the partition with nobody to serve it while
  every other partition on the topic stays uncreated. An agent serves what it
  claimed and refuses the rest.
- **A lost claim is never re-acquired in the same process.** The new epoch would
  be higher, but the agent's in-memory log position would not be: whoever took
  the partition over has been assigning offsets from a position this agent cannot
  see, so writing would reissue offsets that are already taken. The partition stays
  fenced until a restart recovers it. (`Claim` returns `ErrPartitionLost`.)
- **`CreateTopic` claims every partition but tolerates refusals**, so one held
  partition cannot make a whole topic uncreatable by every agent.
- **Superseded-epoch segments are reclaimed by the retention sweep**, not left for
  a separate reaper (this settles open risk 3). They are unreachable, so they are
  deleted unconditionally rather than held until a consumer advances past offsets
  nobody will ask for. Retention's own policies then run over the live segments
  only, where the offset arithmetic is sound.
- **A claim record stores its expiry in whole seconds**, so a TTL under a second
  can be rounded down to nothing and read as already expired. Fine at the 60s
  default; the ownership tests use a TTL comfortably over a second for this reason.
- **`dropUnownedPartitions` runs after the checkpoint load.** The checkpoint is
  agent-private but bucket-wide, so it lists partitions another agent owns;
  keeping them would let this agent serve a log end offset it cannot append to.
- **Deleting a topic also deletes its `_owners/<topic>/` claims.** The epoch is
  not reusable afterwards, deliberately: durable records written under the deleted
  topic's epoch must not be mistaken for current data if the name is reused.
- **Mimir's e2e does not enable idempotence.** Its distributor never sends
  `InitProducerId`, so `test/mimir-e2e.sh` cannot exercise phase 6. That is why
  `github.com/twmb/franz-go` is now a test dependency; see
  `internal/server/franz_idempotence_test.go`, including the forced
  timeout-and-retry that asserts the retry is deduplicated.
- **Liveness and routing are two different objects in one flat prefix**, so
  discovery tells them apart by key shape and skips `_agents/<ns>/checkpoint.json`
  — a private durable object that is not addressed to anyone. Parsing that lives in
  `parseAgentKey`, and a test pins all four cases.
- **An agent that owns nothing still publishes a routing table.** Discovery finds
  agents *by* their table, so a table written only when the owned set is non-empty
  would make a freshly started, idle agent invisible — which is exactly the moment
  another agent needs to know it exists.
- **Two processes sharing an agent id are refused, not merged.** A live record with
  the same agent id and a different node id means two brokers claiming one
  identity; overwriting it would make clients flap between two addresses for one
  broker. The takeover happens when the old record expires, which is the crash
  case.
- **A routing table older than the TTL is ignored while the agent is still live.**
  That combination means the writer is wedged between renewing its liveness and
  republishing its table, and its partitions should be treated as unowned rather
  than routed to a broker that is not making progress.
- **The registry borrows the engine's object-store helpers.** With its own
  timeout it was able to hold engine construction open for the full TTL against a
  store that never answers — a regression the engine's operation timeout exists to
  prevent.
- **`Routing()` deep-copies before overlaying local state.** The snapshot's maps
  are the registry's live maps, shared with the goroutine that refreshes them;
  writing to them was a concurrent map write that the race detector caught.
- **All three `_agents/` objects live under the agent's prefix**, not at it. The
  liveness record at `_agents/<id>` would make that key both an object and a
  directory prefix: object storage does not care, and a filesystem-backed store
  cannot represent it at all. The end-to-end test's local S3 shim is exactly such
  a store, and it failed in a way that looked like an agent bug.
- **Segment names are now the one place a record-shape assumption lives.** A name
  is no longer a bare integer, so every reader parses it through
  `wal.ParseSegmentName` rather than `ParseInt`. A name with no `-e<epoch>` suffix
  parses as epoch 0, which is what keeps a bucket written before phase 2 readable
  and its keys unchanged.

### 13.7 Verification commands

```bash
go build ./...
go vet ./...
go test -race ./...
golangci-lint run ./...
go build -o agent ./cmd/agent && ./test/mimir-e2e.sh   # needs Docker
```

`mimir-e2e.sh` checks that the data is *there*. `mimir-integrity-e2e.sh` checks
that it is *intact*: it pushes a volume whose every sample value is known in
advance, then verifies per-series sample counts and value sums, per-slice counts,
and all of it again after a broker crash, a broker restart, and a full Mimir
restart. `test/logprobe` walks the log with an independent client, so a slow or
buggy consumer is not mistaken for a broker that lost data.

```bash
go build -o agent ./cmd/agent
VOL1_SERIES=60 VOL1_POINTS=400 ./test/mimir-integrity-e2e.sh   # needs Docker
GOOS=linux GOARCH=arm64 go build -o /tmp/kimi-logprobe ./test/logprobe
```

The e2e harness leaves state behind; clean it before a rerun:

```bash
pkill -f s3shim.py
rm -rf /tmp/kimi-mimir-e2e
```

### 13.8 Outstanding operational item

The GitHub Actions workflow (`.github/workflows/ci.yml`) has **never run
remotely**. Watch the first push-triggered run and the nightly/manual Mimir job;
the lint and e2e jobs in particular have only ever been run locally.

---

## 14. What is still missing

Honest inventory, in rough order of how much it matters. Everything here was read
out of the code, not inferred from the roadmap.

### 14.1 There is no replication, so failover is an outage

This is the structural difference from Kafka and everything else follows from it.

Kafka survives a broker dying because in-sync followers *already hold the data*,
so a failover is a metadata change. We survive an agent dying because the data is
already in object storage — a real property, but not the same one. There is exactly
one copy: the Metadata response reports one replica (`internal/protocol/handler.go`),
`CreateTopics` parses `replicationFactor` and discards it
(`internal/protocol/handlers_admin.go`), and `handleUpload` writes one `.log` and
one `.index` per segment with no second writer anywhere.

The consequence is that nothing is *redundant*: there is no second agent that could
take over serving a partition without first claiming it. Fetch on an unowned
partition is now served from object storage (§14.2), which turns a total outage into
a degraded read path, but a consumer still cannot get past the durable frontier
until a peer claims the partition and writes more. That wait is bounded by the claim
TTL — `KIMISTORE_OWNERSHIP_TTL_MS`, 60s by default, renewed every TTL/3 — plus a
consumer group rebalance on top.

So the honest summary: durability is strong, read availability during a failover is
degraded rather than absent, and the tail of new data is delayed by up to one TTL.

### 14.2 Stale reads during failover — implemented

`Fetch` on a partition this agent does not own used to be refused with
`LEADER_NOT_AVAILABLE`. It is now served from object storage when there is anything
durable to serve, and refused only when there is nothing.

The original reasoning was right about the guessing and wrong about the
conclusion. A non-owner's high watermark has to come from somewhere, but the place
it comes from is object storage — the authoritative copy — and reading it there is
not a guess. Refusing instead meant a partition was unreadable from *every* live
agent for as long as its owner was gone, which with no replication was the entire
outage rather than a degraded window.

What the change deliberately does not do:

- **It never reports more than is durable.** The position returned is the log end
  the objects actually hold, so a consumer is never told it is caught up while the
  live owner is still writing. A consumer that needs newer records gets an empty
  response and keeps polling, which is honest.
- **It never consults the local WAL for an unowned partition.** A former owner's
  WAL holds records written before the partition moved away, and serving those
  would hand a consumer data the current owner has since replaced. `ReadBatch`
  keeps its existing WAL-only-if-owned guard.
- **It does not park.** The append latch fires on local writes, so a long poll for
  a partition someone else owns waits out the full timeout and returns nothing.
  Partitions that already have records in object storage skip the park entirely.

Two consequences worth being explicit about:

- **`ListOffsets` splits.** "earliest" is served from object storage, because the
  log start only moves forward, so a stale answer is still a valid one — and
  answering 0 for a log that starts at 90000 is what turns a consumer resetting to
  earliest into an infinite `OffsetOutOfRange` loop. "latest" still returns
  `NOT_LEADER_OR_FOLLOWER`, because a consumer told it is caught up at a stale
  offset stops asking and never learns about the records it was waiting for.
- **Convergence is client-driven.** A successful stale read is not an error, so a
  client has no signal to migrate to the new owner. It will get there on its own
  metadata refresh — `Metadata` already names the peer owner — but nothing here
  actively moves it. Making failover converge faster would need a mechanism Kafka
  does not have.

One supporting fix was needed. Losing a claim now discards this agent's in-memory
position for that partition (`OwnershipConfig.OnLost`). It used to be kept, and it
mixed confirmed and unconfirmed records indistinguishably, so an agent that had just
lost a partition would keep reporting a log end that only its own unflushed WAL
could justify. The WAL directory itself is left on disk deliberately: for a few
seconds after losing a partition it is the only copy of anything written but never
flushed.

### 14.3 Graceful handover is unreachable

`DrainPartition` exists and is correct, but nothing calls it. A partition moves by
crashing or by a clean shutdown; there is no operator path to move one without
paying the TTL. This needs an authenticated mutating endpoint or an operator
protocol — a trust-boundary decision, deliberately not invented here.

Deleting the claim out of band is **not** a substitute: it releases the partition
immediately while the old owner keeps serving until its next renewal, which is
precisely the split-brain window the epoch mitigates but does not close.

### 14.4 Consumer offsets: acked before persisted, and no read-your-writes

`SaveOffset` only writes to an in-memory buffer and acks the client
(`internal/storage/engine.go`); a flusher persists every 5s. An acked commit is
lost on SIGKILL up to 5 seconds later. Separately, `LoadOffset` reads only from
object storage and never consults the buffer, so an `OffsetFetch` issued
immediately after an `OffsetCommit` can answer `-1` — which a client interprets as
"no committed offset" and applies `auto.offset.reset`. After a restart that is a
replay-from-the-start storm rather than a bounded one.

### 14.5 OffsetCommit and OffsetFetch are not fenced

`coordinatorFence` runs at exactly three sites — JoinGroup, SyncGroup, Heartbeat.
Both offset handlers bypass it, so any agent accepts and serves offset commits and
writes to the shared flat `_offsets/` prefix: last-writer-wins, with no generation
fencing. `LeaveGroup` being unfenced is deliberate and documented; this one reads
as an oversight and is not documented anywhere.

### 14.6 Idempotent producer state does not survive a machine change

Only the ID allocator counter is persisted. `producerEntry{epoch, nextSeq,
lastOffset}` is in-memory and recovered from the local WAL tail only, and the epoch
is hardcoded to 0 with no increment path. After a failover to a *different*
machine the dedup window is empty, so a retried `acks=all` batch duplicates. This
follows directly from §14.1 and is accepted rather than fixed.

### 14.7 Kafka feature gaps

- **Transactions absent.** `AddPartitionsToTxn` and `EndTxn` are not in
  `supportedAPIVersions`, so they get `UNSUPPORTED_VERSION` — but
  `InitProducerId` still hands a transactional producer an ID and the
  `transactional_id` is silently discarded. Rejecting cleanly would be safer.
- **No controller, no KRaft.** Every agent reports itself as controller. No
  inter-agent TLS or authentication.
- **No flexible/tagged-field versions.** Every ceiling is the newest non-flexible
  one (Metadata v7 and so on), which will eventually block client upgrades.
- **No multi-tenancy or ACLs.** `ErrGroupAuthorizationFailed` (30) is returned for
  *any* coordinator error, which is misleading rather than an authorization check.
- **Timestamp-based ListOffsets** is not implemented; it answers log end so callers
  make progress.

### 14.8 One latent footgun

With `KIMISTORE_REQUIRE_LEASE=false` against a store that cannot do conditional
writes, `Claim` returns early without recording anything, so `Owns()` reports false
for *every* partition: writes still succeed and every Fetch is answered
`LEADER_NOT_AVAILABLE`. Unreachable with defaults, but it is a broker that accepts
writes and serves no reads.
