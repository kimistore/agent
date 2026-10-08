## Compatibility Guide

This server implements a subset of the Kafka Protocol. It is designed to be
compatible with standard Kafka clients and supports both **Simple Consumer**
and **Consumer Group** workflows.

Version ceilings differ per API. Several APIs reach the newest non-flexible
version, and others stop at v0 for a documented reason. See "Version
ceilings" below for each ceiling and the reason it sits where it does. The
authoritative list is the `Protocol versions advertised` line the agent logs
at startup, and the per-API table below.

### Supported Features
*   **Protocol Versions**: Kafka 0.11-era APIs, with several serving later
    versions. `Metadata` reaches v7 and `InitProducerId` v1, both newer than
    0.11. `ApiVersions` advertises exactly what is implemented, and any other
    version of a known API, or an unknown API, is answered `UNSUPPORTED_VERSION`
    rather than dropped.

    | API | Versions |
    | :--- | :--- |
    | Produce | 0-3 |
    | Fetch | 0-5 |
    | ListOffsets | 0-2 |
    | Metadata | 0-7 |
    | OffsetCommit | 0 |
    | OffsetFetch | 0-1 |
    | FindCoordinator | 0 |
    | JoinGroup | 0-1 |
    | SyncGroup / Heartbeat / LeaveGroup | 0 |
    | CreateTopics / DeleteTopics | 0 |
    | InitProducerId | 0-1 |
    | ListGroups / DescribeGroups | 0 |
    | SaslHandshake / SaslAuthenticate | 0-1 / 0-1 |
*   **Message Format**:
    *   MessageSet V0/V1 (legacy), including GZIP, Snappy (with Kafka's length
        prefix) and LZ4 compression.
    *   RecordBatch V2 (Kafka 0.11+), in **both** encodings: bare, as
        `segmentio/kafka-go` sends it, and wrapped in an outer message set
        entry, as the Java client, librdkafka and sarama send it. The record
        count is read from the batch header, so the log end offset advances by
        one per record rather than one per batch.
*   **Fetched batches are always re-framed bare.** `segmentio/kafka-go`
    assumes the bare form in *both* of its decoders, so data written by a Java
    or librdkafka producer would otherwise be unreadable to a client built on
    it. Both framings are valid Kafka. See `wal.UnwrapBatches`.
*   **Decompression**: Automatic decompression of GZIP, Snappy, and LZ4 compressed message batches for accurate offset tracking.
*   **Durability**: Acknowledged writes follow Kafka's `acks` semantics, with
the single-node agent treating `acks=all` as the strongest guarantee it can
offer:

    | `acks` | Behaviour |
    | :--- | :--- |
    | `0` | No response expected. The batch is written to the WAL but **not** fsynced. Fastest; a crash may lose it. |
    | `1` | Batch is fsynced locally before the offset is returned. A successful ack means the data survived a local crash, but not the loss of the agent. |
    | `-1` (all) | Batch is fsynced locally **and** the offset is withheld until the segment holding it is in object storage (**posture D2**). A successful ack then means the data is recoverable from the bucket. The wait is bounded by the producer's timeout (capped at 30s); on expiry the produce returns `REQUEST_TIMED_OUT` rather than a false ack. |

    Concurrent appends share a single flush (group commit), so many producers
    writing in parallel do not each pay for their own fsync. For `acks=all`,
    a background flush seals and uploads a waiting partition every
    `KIMISTORE_FLUSH_INTERVAL_MS`, which is both the coalescing window for
    object-store PUTs and the extra ack latency. `acks=all` without idempotent
    producers can duplicate a retried batch. With an idempotent producer the
    retry is deduplicated; see "Idempotent producers" below.
*   **Consumer Groups**: Features a built-in "Lite" Group Coordinator supporting:
    *   Dynamic partition assignment and load balancing (`JoinGroup`, `SyncGroup`, `LeaveGroup`).
    *   Background session/heartbeat tracking (`Heartbeat`).
    *   Persistent offset commits saved cheaply to object storage (`OffsetCommit`, `OffsetFetch`).
*   **Discovery**: `Metadata` and `FindCoordinator` requests allow clients to auto-discover brokers, topics, partitions, and group coordinators.
*   **Data Persistence**: Low-latency local Write-Ahead Log (WAL) with transparent asynchronous segment offloading to AWS S3 or compatible object stores.

### Version ceilings

Every advertised version is implemented exactly as the schema declares it.
Kafka serialises message fields in declaration order, so the layouts below were
taken from Apache Kafka's own `*.json` message definitions rather than from
any client's decoder.

**Produce stops at v3.** v3 is where magic 2 became legal, and magic 2 is the
record batch format. Capping Produce lower looks attractive: a v0 response
has no `ThrottleTimeMs` field, so it cannot be misordered. It is a trap.
Offer a client only Produce v0-v2 and it falls back to magic 1, which has no
header field, so every Kafka record header the producer set is silently
discarded. Grafana Mimir keeps the wire format of each write in a record
header; measured against a real Mimir 3.2.1, a Produce v0 ceiling makes its
ingester fail to parse every record it consumes
(`Remote Write 2.0 field Symbols in non-Remote Write 2.0 message`) because it
sees version 0 for everything. v3 is also the newest Produce version before the
flexible (tagged-field) encoding, which is not implemented here.

**Heartbeat and LeaveGroup stop at v0.** The target client decodes those two
with `ErrorCode` before `ThrottleTimeMs`, the reverse of the schema, so
offering v1+ would hand it a misaligned response. v0 has no throttle field, so
the two readings coincide.

**Metadata goes to v7.** v7 is the newest non-flexible version, and the only
field it adds over v6 is `LeaderEpoch` -- the ownership epoch of the agent that
leads the partition, which is how a client detects that the leader it cached has
been replaced. v8 adds `TopicAuthorizedOperations`, a bitmask of the
operations a client may perform on each topic. The agent enforces topic
authorization from its own ACL rules (see "Authorization" below), but it
does not emit that bitmask, so v7 is where the ceiling stops. Note that
`cluster_id` is a **v2** field, not v1: a v1 client reads whatever sits
where the controller id belongs, so emitting it early desynchronises
every field after it.

**Fetch goes to v5**, which adds `log_start_offset` so a client can find the
log start without a separate ListOffsets round trip.

**Fetched RecordBatches are always re-framed bare.** Both Go Kafka clients
assume the bare form in *both* of their decoders, so data written by a Java or
librdkafka producer would otherwise be unreadable to the client this agent
exists to serve. Both framings are valid Kafka: a RecordBatch is
self-describing either way.

### When a client does not negotiate

A request above an advertised ceiling is answered `UNSUPPORTED_VERSION` in a
response shaped like a real response of that version, so a well-behaved client
downgrades and retries instead of dropping the connection. Every such refusal
increments `kimistore_unsupported_api_versions_total{api, version}`, and the
startup log prints the advertised version table with the reason for each
ceiling. A client that reads ApiVersions and adapts should never generate that
counter; if yours does, the label names the API and the version your client
demands.

### Limitations (What WON'T work)
1.  **Transactions**: Transactional producing (`AddPartitionsToTxn`, transaction
    markers, `EndTxn`) is not implemented, and a transactional producer will
    fail when it opens a transaction. Idempotent (non-transactional) producing
    **is** supported: `InitProducerId` allocates an id, and produce batches are
    deduplicated by producer id, epoch and sequence. Producer state is rebuilt
    from the local WAL tail on restart, so a fresh-machine restart resets it.
2.  **Replication**: Multi-broker data replication (partition leaders and ISRs)
    is not implemented. The agent operates as a single-node broker.
3.  **Flexible (tagged-field) message versions**: Metadata v9+, Fetch v12+ and
    every other flexible version are not decodable here, so they are not
    advertised and are refused on arrival.
4.  **Offset commits are acknowledged before they are persisted.** A commit is
    flushed to object storage on a background loop. A crash in between loses
    the commit and the group replays from its previous position, which is safe
    but not exactly-once.

### Client Configuration Examples

#### Python (confluent-kafka / librdkafka)
```python
conf = {
    'bootstrap.servers': 'localhost:19092',
    'group.id': 'my-group',
    'api.version.request': True,
    'auto.offset.reset': 'earliest',
}
c = Consumer(conf)
c.subscribe(['my-topic'])
```

#### Java (KafkaClient)
```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:19092");
props.put("group.id", "my-group");
props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
consumer.subscribe(Collections.singletonList("my-topic"));
```

#### CLI (kcat)
```bash
# Produce Messages:
echo "hello world" | kcat -P -b localhost:19092 -t my-topic

# Consume using a Consumer Group:
kcat -b localhost:19092 -G my-group my-topic
```

## Detailed Feature Matrix

| Feature Category | Feature | Status | Notes |
| :--- | :--- | :--- | :--- |
| **Core Protocol** | Produce API (V0-V3) | ✅ Supported | See the throttle note above |
| | Fetch API (V0-V5) | ✅ Supported | |
| | ListOffsets (V0-V2) | ✅ Supported | earliest offset reflects retention, not a hardcoded 0 |
| | Metadata (V0-V7) | ✅ Supported | |
| | ApiVersions (V0) | ✅ Supported | Higher versions are refused so the client retries at v0 |
| **Messaging** | MessageSets (V0, V1) | ✅ Supported | Legacy format |
| | RecordBatch (V2) | ✅ Supported | bare and message-set-wrapped encodings; record count read from the batch header |
| | Compression | ✅ Supported | GZIP, Snappy, and LZ4 decompressed for counting |
| **Consumption** | Simple Consumer | ✅ Supported | `assign()` partitions manually |
| | Multi-record fetch | ✅ Supported | fills the client's byte budget |
| | Consumer Groups | ✅ Supported | `subscribe()` is fully operational via Lite Coordinator |
| | Offset Commit | ✅ Supported | Group progress committed/loaded from S3 |
| **Durability** | Local Persistence | ✅ Supported | Synchronous WAL |
| | Acked-write fsync | ✅ Supported | Group-committed; `acks=1` is local, `acks=all` waits for object storage |
| | Idempotent producer | ✅ Supported | `InitProducerId`; duplicate retries answered with their original offset |
| | S3 Offload | ✅ Supported | Asynchronous upload |
| | S3 Recovery | ✅ Supported | Transparent fallback to S3 |
| **Durability** | Log position across restart | ✅ Supported | checkpoint + manifest + segment-tail recovery |
| | Active segment on shutdown | ✅ Supported | sealed and uploaded before exit |
| **Reliability** | Replication | ❌ No | Single node only |
| | ISR/HW | ❌ No | Always ISR=1, HW=Max |
| **Authentication** | SASL/SCRAM-SHA-256 | ✅ Supported | Offered only when credentials exist under `_scram/` |
| | SASL/SCRAM-SHA-512 | ✅ Supported | Offered only when credentials exist under `_scram/` |
| | SASL/PLAIN | ✅ Supported | One shared credential from `SASL_USERNAME` and `SASL_PASSWORD` |
| | `SaslAuthenticate` V1 | ✅ Supported | Server-final-message framing |
| **Authorization** | Topic ACLs | ✅ Supported | Rules under `_acl/`; deny overrides allow; operations Read, Write, Describe, Create, Delete, Alter, All |
| | `Describe` on topics | ✅ Enforced | Refused with `TOPIC_AUTHORIZATION_FAILED` |
| | `Describe` on consumer groups | ❌ No | Groups are not covered by topic rules |
| | `DescribeGroups` authorization | ❌ No | The v8 `TopicAuthorizedOperations` bitmask is not emitted |

### Authentication

The agent supports three SASL mechanisms. It advertises only the mechanisms
it can actually complete.

| Mechanism | Configured by | Advertised when |
| :--- | :--- | :--- |
| `PLAIN` | `SASL_USERNAME` and `SASL_PASSWORD` | Both variables are set |
| `SCRAM-SHA-256` | `kimistore-credential create` | A credential exists under `_scram/` |
| `SCRAM-SHA-512` | `kimistore-credential create` | A credential exists under `_scram/` |

With no credentials and no `PLAIN` variables, the agent serves every request
without authentication. `ApiVersions` reflects the mechanisms actually on
offer, so a client never negotiates a mechanism the agent cannot complete.

`PLAIN` compares the supplied username and password against one shared pair.
It sends the password in the clear unless the connection uses TLS. Use it only
on a trusted network, or use SCRAM.

SCRAM credentials live under `_scram/` in the object store. The store holds the
stored key, the salt, and the iteration count. It never holds the password, so
a read of that prefix does not reveal it. `SaslAuthenticate` supports v0 and v1.

### Authorization

Topic ACLs are optional. Rules live under `_acl/`. With no rules the agent
allows every request.

The agent enforces topic authorization on `Produce`, `Fetch`, `Metadata`, and
`CreateTopics` and `DeleteTopics`. A denied request is answered with
`TOPIC_AUTHORIZATION_FAILED`.

Each operation maps to one API:

| Operation | Enforced on |
| :--- | :--- |
| `Read` | `Fetch` |
| `Write` | `Produce` |
| `Describe` | `Metadata` |
| `Create` | `CreateTopics` |
| `Delete` | `DeleteTopics` |
| `Alter` | Stored and accepted, but no handler consults it |
| `All` | Every operation above |

A producer needs `Describe` before it can write. A grant of `Write` alone fails
at metadata time, and the client never reaches the write check.

Three gaps are deliberate for this release:

- The agent does not enforce `Describe` on consumer groups. Topic rules do not
  apply to a group coordinate.
- The agent does not emit the `TopicAuthorizedOperations` bitmask that
  `Metadata` v8 and `DescribeConfigs` use to advertise per-topic operations.
- `Alter` is accepted as a rule operation, but the agent implements neither
  `AlterConfigs` nor `DeleteRecords`, so a rule that names `Alter` has no
  effect.

See "Authorization" in the [`README.md`](README.md) for the rule syntax and the
`kimistore-credential acl` command.

### Operational guarantees that affect the protocol

* **One writer per partition.** The agent claims each `(topic, partition)` in
  object storage with a compare-and-swap before writing to it, and answers a
  produce for a partition it does not hold with `NOT_LEADER_OR_FOLLOWER`. Two
  agents must never interleave on one partition: they would assign the same
  offsets and overwrite each other's segments. A partition that is already held
  by a live agent is skipped rather than fatal, so the partitions an agent *can*
  have stay servable. If the store cannot enforce conditional writes,
  `KIMISTORE_REQUIRE_LEASE=false` downgrades the fence to a startup warning.
  `KIMISTORE_PARTITION_OWNERSHIP=false` reverts to one bucket-wide claim that
  refuses a second agent outright.
  * **Every agent must have a distinct `KIMISTORE_AGENT_ID`.** Two agents sharing
    one id cannot tell each other apart from the claim record, because the record
    names the id both of them use. The agent therefore also compares epochs: a
    live claim at an epoch above the one it holds is a takeover, not a renewal, and
    it is refused with an error naming the variable. Without that check the two
    agents hand the partition back and forth forever -- the epoch climbs on every
    renewal tick, both keep acknowledging writes, and their log end offsets
    diverge. An *expired* claim above its own epoch is still taken over, which is
    the ordinary failover after the previous holder died.
  * **Clients are routed to the agent that owns a partition.** `Metadata` reports
    every live agent and names each partition's leader, so a client writes to and
    reads from the agent holding it. A partition whose owner is gone is reported as
    `LEADER_NOT_AVAILABLE`, and a `Fetch` or `ListOffsets` for a partition this
    broker does not own is refused -- an agent that does not own a partition cannot
    know its log end offset, so any answer it gave would be a guess. `Metadata` is
    advertised up to v7, which is the only version past v6 that is not flexible; it
    adds `LeaderEpoch`, so a client can detect a leader change.
  * **An orderly shutdown hands the partitions over.** On `SIGTERM` the agent stops
    accepting, then for each partition it owns: seals the active segment, waits until
    the durable frontier covers the log end, writes the partition manifest, and only
    then releases the claim. The order is load-bearing, because the claim is gone the
    moment it is released. A partition whose tail cannot be made durable within the
    budget *keeps* its claim and lets it expire, so a slow object store produces a
    slower failover rather than lost records. The whole run is bounded by a 25 second
    deadline, so set the container grace period above that. A `SIGKILL` skips all of
    this: the claims are released in bulk and the next owner recovers by reconciling
    against object storage.
  * **The manifest write is fenced against the object store.** Before writing a
    partition manifest the agent re-reads its claim rather than trusting its local
    view, because the manifest is an unconditional put and a stale writer would
    otherwise overwrite the new owner's epoch and log end with its own. A refused
    write increments `kimistore_manifest_writes_rejected_total` and drops the
    partition from the pending set. The append path itself still trusts the local
    view, which refreshes every third of the claim time to live, so an agent that has
    lost a claim can accept writes briefly before it notices.
* **Groups are coordinated by one agent, and it is checked.** `FindCoordinator`
  picks the agent by rendezvous hashing of the group id over the live set, and
  `JoinGroup`, `SyncGroup` and `Heartbeat` answer `NOT_COORDINATOR` on any agent
  that is not the winner. Group state is in memory, so a coordinator change is a
  full rebalance; consumer offsets are durable in object storage, so the rebalance
  does not cost a replay. `LeaveGroup` is deliberately not fenced.
* **Durable state carries an ownership epoch.** The per-partition manifest, the
  checkpoint and the segment key (`<baseOffset>-e<epoch>.log`) all record the
  epoch that wrote them. An agent whose epoch is behind the one it finds on
  startup refuses to serve that partition, which closes the window between one
  owner stopping and its replacement starting. Putting the epoch in the segment
  key is what keeps a superseded writer's late upload from overwriting its
  successor's segment at the same base offset.
* **Every storage call is bounded.** Object-store operations are limited by
  `KIMISTORE_S3_TIMEOUT_MS`, and a client that disconnects cancels the read it
  started. A client therefore sees a retriable error rather than a stalled
  connection, which matters because a stalled connection stops heartbeats and
  commits and shows up as a rebalance storm.

New metrics for these: `kimistore_partitions_owned`,
`kimistore_writer_epoch`, `kimistore_ownership_claim_failures_total`,
`kimistore_ownership_renewal_failures_total`,
`kimistore_ownership_refused_writes_total`, `kimistore_agents_live`,
`kimistore_routing_brokers`, `kimistore_routing_age_seconds`,
`kimistore_routing_publish_conflicts_total`,
`kimistore_routing_inconsistent_tables_total`,
`kimistore_routing_duplicate_node_ids_total`,
`kimistore_routing_inventory_failures_total`, `kimistore_agent_live`,
`kimistore_coordinator_groups_total`, `kimistore_coordinator_refused_total`,
`kimistore_object_store_timeouts_total`. The bucket-global lease gauges
(`kimistore_lease_owned`, `kimistore_lease_epoch`,
`kimistore_lease_renewal_failures_total`, `kimistore_lease_refused_writes_total`)
remain for deployments that turn ownership off.
