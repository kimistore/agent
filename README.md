# Kimistore

**Kimistore** is a lightweight, cloud-native streaming platform built in Go. It is designed to be protocol-compatible with Apache Kafka but re-architected to run on top of commodity Object Storage (like S3) without local disks or heavy dependencies like ZooKeeper.

This project is a Apache Kafka® - compatible data streaming agent. The goal is to separate compute from storage, allowing for stateless agents that can scale instantly while durable data resides cheaply and safely in object storage.

Each agent claims each partition it writes and publishes where it can be reached, so several agents can share one bucket and clients are routed to the one that owns each partition. See [Single-Writer Fence](#2b-single-writer-fence) and [Routing](#2f-routing).

## 🧪 Tests

```bash
make test        # unit tests
make race        # unit tests under the race detector
make cover       # coverage summary and HTML report
make lint        # golangci-lint
make ci          # everything CI runs on a pull request
make e2e         # the Mimir end-to-end test (needs Docker)
```

The end-to-end test runs a real Grafana Mimir with this agent as its Kafka
landing zone, pushes remote-write samples through it, reads them back with a
PromQL query, then restarts the agent with an **empty local WAL** to confirm the
data survives. It starts its own local S3 shim, so nothing external is required
beyond Docker.

## 🚀 Project Goal

The primary objective is to build a "serverless" Kafka-compatible broker that:
1.  **Speaks Kafka Protocol**: Works with standard Kafka clients (kcat, Java, Python, Go consumers/producers).
2.  **uses Object Storage**: Persists all data to S3 (or compatible stores) instead of local disks (`EBS`).
3.  **Stateless Architecture**: Agents acts as a proxy/cache layer. State is in the object store.

## ✅ Implemented Features

As of the current version, the following core components are working:

### 1. Storage Layer (S3-Backed WAL)
*   **Write-Ahead Log (WAL)**: Records are locally appended to partitioned WAL files for low-latency writes.
*   **Durability**: With `acks=1`, the batch is fsynced to stable storage *before* the offset is returned. With `acks=all`, the offset is withheld until the segment holding it is in object storage, so an acknowledged offset is recoverable from the bucket (see "Durability of acks=all" below). `acks=0` skips the flush. Concurrent appends share one flush via group commit, so parallel producers do not each pay a separate fsync.
*   **Crash Recovery**: A torn trailing record (the normal result of a crash mid-write) is truncated back to the last intact record on startup, and the partition continues serving from there instead of failing to open.
*   **Asynchronous Uploads**: Background routines automatically roll segments (e.g., at 1MB or time intervals) and upload them to the configured Object Store.
*   **Unified Read Path**: Consumers read from the "Hot" WAL (RAM/Local Disk) for real-time tailing and seamlessly fallback to "Cold" Object Storage for historical reads.
*   **High Watermark**: Correct handling of log end offsets to ensure consumers waiting for data receive empty success responses rather than errors.

### 1b. Retention
Time and size based retention reclaim sealed segments from Object Storage.

*   **Enabled by configuration**: Retention is inert until `KIMISTORE_RETENTION_MS` and/or `KIMISTORE_RETENTION_BYTES` are set.
    *   `KIMISTORE_RETENTION_MS` — max age of a segment, in milliseconds.
    *   `KIMISTORE_RETENTION_BYTES` — max bytes per partition (`-1` for unlimited).
    *   `KIMISTORE_RETENTION_CHECK_MS` — sweep interval, default `300000` (5 minutes).
*   **Partition-scoped listing**: Each sweep lists only the known `topic/partition/` prefixes, instead of enumerating the whole bucket every interval. The previous whole-bucket scan was an expensive, throttle-prone operation that grew with total stored data rather than with the data being reclaimed.
*   **Consumer-aware**: A segment is never reclaimed while it still holds an offset at or above the lowest offset any consumer group has committed. The newest segment is always retained, since it cannot be proven fully consumed.

### 2. Kafka Protocol Support
We implement a subset of the Kafka binary protocol sufficient to support standard producers and consumers.

*   **Producing**: Supports batch production of messages to specific topics and partitions.
*   **Consuming**: Supports fetching messages with offset management, including **long polling**, and fills the client's requested byte budget with as many records as fit rather than one per round trip. When a consumer is caught up and asks for data (`minBytes > 0`), the request parks on an append signal for up to `maxWaitMs` (capped at 1s) instead of returning empty immediately. This stops caught-up consumers from re-polling in a tight loop.
*   **Dynamic Partitions**: Topics and partitions are auto-discovered from the underlying storage layout.

### 3. Consumer Groups (Lite Coordinator)
A built-in "Lite" Group Coordinator allows multiple consumers to work together to consume topics:
*   **Membership Management**: Handles `JoinGroup`, `LeaveGroup`, and `Heartbeat` life-cycles.
*   **Session Reaper**: A background reaper evicts members that stop heartbeating past their `session.timeout.ms`, advances the group generation, and re-elects a leader if the leader died. A crashed consumer therefore stops holding its partitions, and the group rebalances without operator action. Restored members from a checkpoint are treated as dead, since their connections died with the previous process.
*   **Bounded Rebalancing**: `SyncGroup` followers wait for the leader's assignment with a bounded timeout rather than indefinitely, so a leader that dies mid-rebalance cannot pin followers, goroutines, or connections.
*   **Leader-Based Assignment**: Delegates partition assignment to the consumer group leader (standard Kafka client behavior).
*   **Rebalancing**: Supports adding/removing consumers dynamically with minimal disruption.
*   **Partition Awareness**: Correctly distributes partitions (e.g., 2 partitions -> 2 consumers) across the group.
*   **Durable Commits**: `OffsetCommit` is acknowledged immediately and flushed to Object Store on a background loop. Failed flushes are retried rather than dropped, and a bounded retry runs during shutdown, so a transient object-store error cannot silently discard a commit.

### 2b. Single-Writer Fence
Two agents writing one partition do not queue behind each other. They each assign offsets from their own recovered state and write segments to keys derived from those offsets, so they overwrite each other and the failure is **silent data loss** — no error, no crash, just a log that is missing records.

The agent therefore claims each partition before it writes to it:

* **Claim**: An ownership object (`_owners/<topic>/<partition>`) inside the same bucket, acquired with a compare-and-swap (`If-None-Match: *` to create, `If-Match: <etag>` to renew). Object storage enforces both preconditions server-side, so two agents racing for a partition cannot both win.
* **Epoch**: The claim carries a monotonically increasing epoch, per partition. It goes into the segment key (`<baseOffset>-e<epoch>.log`) and into the partition manifest.
* **Segment keys**: the epoch is in the key because a superseded writer's upload can arrive *after* its successor has written at the same base offset — seal a segment, lose the claim while the upload is in flight, and the two collide. With the epoch in the key they land side by side instead, and the stale copy is unreachable, so retention reclaims it. A read at a given offset always resolves to the highest epoch there.
* **Renewal**: A background loop renews every `TTL/3`. A renewal that fails is not immediately fatal — object stores have bad minutes — but once failures have aged past the TTL the agent stops writing to that partition, because at that point another agent may legitimately have taken it. Losing one partition does not affect the others.
* **Handover**: A graceful shutdown releases every claim, so replacements start immediately instead of waiting out the TTL. A crashed agent blocks only its own partitions, for at most the TTL.
* **Refusal**: An unowned partition turns into refused writes and a `NOT_LEADER_OR_FOLLOWER` error, which is what tells a producer to refresh its metadata and retry against the agent that owns it.
* **Fail closed**: a claim this agent cannot renew is never re-acquired in the same process. The epoch would be newer, but the agent's in-memory log position is not, so writing would reissue offsets that are already taken. The partition stays fenced until a restart recovers it.

Requires conditional-write support (all current S3-compatible stores). `KIMISTORE_REQUIRE_LEASE=false` downgrades a store without it to a loud warning.

Setting `KIMISTORE_PARTITION_OWNERSHIP=false` restores the previous behaviour exactly: one bucket-global claim at `_meta/lease.json`, taken with `KIMISTORE_WRITER_LEASE`, refusing a second agent outright. The two are alternatives — there is one fence at a time, because two overlapping claims would mean two epochs per write and only one of them would fence anything.

### 2f. Routing
Partition ownership makes it *safe* for several agents to share a bucket. Without routing they would be unreachable, so each agent also announces itself and what it owns:

* **Liveness**: `_agents/<agent-id>/liveness` carries the agent's broker id and advertised address, renewed with a compare-and-swap. Two brokers claiming one identity is refused and logged rather than merged -- last-writer-wins would make clients flip between two addresses for one broker.
* **Routing table**: `_agents/<agent-id>/routing` lists the partitions that agent owns with their ownership epochs. Written when the owned set changes, not on a timer, so a topic created a moment ago is advertised immediately rather than after a refresh interval.
* **`Metadata`**: brokers are the live agents, and each partition's `Leader` is the node id of the agent that owns it. A partition whose owner is missing, or whose table has gone stale, is reported as `LEADER_NOT_AVAILABLE` with `Leader = -1` -- which is what makes a client refresh its metadata and retry, instead of giving up on the topic. `Metadata` v7 also carries `LeaderEpoch`, so a client can tell that the leader it cached has been replaced.
* **Owner-scoped reads**: a `Fetch` or `ListOffsets` for a partition this agent does not own is refused rather than answered. This is not strictness for its own sake: an agent that does not own a partition does not know its log end offset, so any high watermark it reported would be a guess, and a guess makes a consumer either stop early or skip records.
* **Long-poll only works on the owner**, because the wake-up latch fires on this process's own appends. A fetch on a partition this agent does not serve returns immediately rather than parking for an append that will never come.

### 2g. Group Coordination
Consumer groups are HA the same way partitions are: assigned by hashing, then fenced at the point of use.

* **Selection**: `FindCoordinator` picks the agent by rendezvous hashing of the group id over the live set. Two agents with the same view compute the same answer, and when an agent leaves only the groups it coordinated move -- not every group in the cluster.
* **The fence**: `JoinGroup`, `SyncGroup` and `Heartbeat` re-check that this agent is the group's coordinator and answer `NOT_COORDINATOR` if it is not. A client treats the `FindCoordinator` answer as a hint and re-resolves, which is what makes two coordinators for one group unreachable even while two agents' views briefly differ.
* **No grace window**: an agent that cannot renew its liveness record stops coordinating *immediately*, unlike a partition owner which waits out the TTL. A partition owner that loses its claim is still fenced by the epoch in every segment name and manifest; a coordinator has no such token, so it may already have been replaced.
* **Group state is in memory.** It is not written to the checkpoint any more, so a coordinator change is a full rebalance -- the cost D-4 accepts. **Consumer offsets are not affected**: they are durable in object storage, so a consumer rejoining after a coordinator change resumes where it left off rather than replaying the topic.
* `LeaveGroup` is deliberately not fenced, so a member can always tell someone it is going away even if the agent it was talking to has changed.

Each agent still needs a **distinct advertised address** -- clients are redirected to it, so a wrong `KIMISTORE_ADVERTISED_HOST` moves them off a working broker onto a broken one.

### 2c. Bounded Storage Calls
Every object-store call is bounded by `KIMISTORE_S3_TIMEOUT_MS`, and the request context is threaded from the connection through the protocol handlers into the storage layer. A slow object store therefore produces a failed request the client can retry, instead of a handler goroutine that never returns. That matters more than it sounds: a hung read occupies one of the connection's in-flight slots, and once those fill, the client stops sending heartbeats and commits and the group rebalances around what is really a storage stall.

On shutdown, in-flight calls are cancelled once the final offset flush and checkpoint have landed, so a wedged store cannot hold the process open.

### 2d. Durable Metadata Layout
The log position and the checkpoint are not bucket-global singletons, so a second agent cannot overwrite them:

* **Per-partition manifests**: `_topics/<topic>/_manifest/<partition>` holds one partition's log end offset, log start offset and segment inventory, stamped with the ownership epoch. An agent rewrites only the partitions whose position moved -- one PUT per changed partition -- so a checkpoint no longer republishes the whole log, and recovery discovers the partitions with one bounded `LIST` under `_topics/`. A partition the agent does not own is never written, so it cannot replace another agent's position with its own.
* **Per-agent checkpoint**: `_agents/<agent-id>/checkpoint.json` carries committed offsets and coordinator state. Namespacing it by `KIMISTORE_AGENT_ID` (default: hostname) is what keeps a second agent sharing the bucket from replacing the first one's checkpoint. A bucket written by an older agent keeps its single `_meta/checkpoint.json` and `_meta/manifest.json`; they are read once on upgrade and re-persisted in the new shape.

### 2e. Durability of acks\=all (posture D2)
`acks=1` fsyncs locally and acknowledges; the strongest guarantee the agent can offer is reserved for `acks=all`. A record written with `acks=all` is **not acknowledged until the segment holding it is in object storage**, so an offset the producer was told about is one it can recover.

* A background flush loop seals the active segment of any partition whose `acks=all` producers are waiting, every `KIMISTORE_FLUSH_INTERVAL_MS`. That interval is both the coalescing window for object-store PUTs and the upper bound on the extra ack latency.
* The uploader reports each stored segment's offset range back to the engine, which advances a per-partition durable watermark. Waiters are released when it covers their offset.
* The watermark only moves contiguously: if the upload pool finishes a later segment first, it is held until the gap ahead of it fills, so a producer can never be acknowledged against a hole.
* If the segment does not reach object storage within the producer's own timeout (capped at 30s), the produce returns `REQUEST_TIMED_OUT` rather than a false acknowledgement. The record is still in the local WAL, so the client retries; an idempotent producer's retry is deduplicated (see 2f), and a non-idempotent one may duplicate it.

### 2f. Idempotent Producers
`InitProducerId` is implemented, so an idempotence-enabled client (Grafana Mimir's franz-go distributor, for one) keeps idempotence on and produces batches carrying a producer id, epoch and sequence number.

* Each partition tracks, per producer, the epoch, the next expected sequence and the offsets its recent batches were assigned (the last five, as Kafka does). A retried batch is recognised by its base sequence and answered with the offset it already occupies instead of being appended a second time. This is what closes the duplicate window that `acks=all` opens when an ack times out.
* A stale epoch is fenced (`INVALID_PRODUCER_EPOCH`); a sequence ahead of the expected one is rejected (`OUT_OF_ORDER_SEQUENCE_NUMBER`) unless it is merely pipelined out of order, in which case it waits briefly for the gap to fill.
* Producer ids come from a persisted monotonic allocator, so a restart cannot reissue an id and mistake a new producer for an old one. Recent sequence state is rebuilt from the local WAL tail on startup, so a retry after a same-machine restart is still deduplicated. After a fresh-machine restart the state resets and an idempotent retry can duplicate; Mimir tolerates that, and it is the documented limit of this phase.
* Transactions (`AddPartitionsToTxn` and friends) are not implemented. A transactional producer gets an id but fails when it opens a transaction.

## 📡 Supported Kafka APIs

The following Kafka API Keys are currently implemented:

| API Name | API Key | Description | Status |
| :--- | :---: | :--- | :--- |
| **Produce** | 0 | Send messages to the broker. | ✅ Active (V0-V3) |
| **Fetch** | 1 | Consume messages from valid offsets. | ✅ Active (V0-V5) |
| **ListOffsets** | 2 | Get earliest/latest offsets. | ✅ Active (V0-V2) |
| **Metadata** | 3 | Discover brokers, topics, and dynamic partitions. | ✅ Active (V0-V6) |
| **OffsetCommit** | 8 | Save consumer group offsets to Object Store. | ✅ Active (S3) |
| **OffsetFetch** | 9 | Retrieve consumer group offsets from Object Store. | ✅ Active (S3) |
| **FindCoordinator** | 10 | Locate the group coordinator. | ✅ Active |
| **JoinGroup** | 11 | Register a consumer member. | ✅ Active (V0-V1) |
| **Heartbeat** | 12 | Keep member session alive. | ✅ Active |
| **LeaveGroup** | 13 | Graceful consumer shutdown. | ✅ Active |
| **SyncGroup** | 14 | Distribute partition assignments. | ✅ Active |
| **DescribeGroups** | 15 | Get detailed group/member info. | ✅ Active |
| **ListGroups** | 16 | List active consumer groups. | ✅ Active |
| **ApiVersions** | 18 | Negotiate protocol support. | ✅ Active (V0) |
| **CreateTopics** | 19 | Create new topics. | ✅ Active |
| **DeleteTopics** | 20 | Delete topics. | ✅ Active |
| **InitProducerId** | 22 | Allocate an idempotent producer id. | ✅ Active (V0-V1) |
| *any other key* | — | Answered `UNSUPPORTED_VERSION`, connection preserved | ✅ |

## 🛠 Usage

### Prerequisites
*   Go 1.27+
*   S3-compatible bucket
*   `kcat` (recommended for testing)

This is the check that matters, because Mimir 3.x uses `twmb/franz-go`, not the
`segmentio/kafka-go` the rest of the test suite uses. It is how two real
protocol bugs were found.

### Configuration

| Variable | Default | Purpose |
| :--- | :--- | :--- |
| `KIMISTORE_LISTEN_ADDR` | `:19092` | Address the broker socket binds to |
| `KIMISTORE_ADVERTISED_HOST` | hostname | Address reported in Metadata / FindCoordinator. **Must be reachable by clients** -- this is what they dial. |
| `KIMISTORE_ADVERTISED_PORT` | port of the listen address | As above |
| `KIMISTORE_METRICS_ADDR` | `:9091` | Prometheus endpoint |
| `KIMISTORE_WAL_DIR` | `./data/wal` | Local write-ahead log. Safe to lose: the shutdown path seals and uploads every segment. |
| `S3_BUCKET` / `AWS_REGION` / `S3_ENDPOINT` | `kimistore` / `us-east-1` / unset | Object store |
| `SASL_USERNAME` / `SASL_PASSWORD` | unset | When set, SASL/PLAIN is required and advertised |
| `KIMISTORE_RETENTION_MS` | `0` (disabled) | Max segment age |
| `KIMISTORE_RETENTION_BYTES` | `-1` (unlimited) | Max bytes per partition |
| `KIMISTORE_RETENTION_CHECK_MS` | `300000` | Sweep interval |
| `KIMISTORE_S3_TIMEOUT_MS` | `30000` | Deadline for a single object-store request |
| `KIMISTORE_PARTITION_OWNERSHIP` | `true` | Claim each partition separately, so several agents can share a bucket |
| `KIMISTORE_OWNERSHIP_TTL_MS` | `60000` | How long a partition claim survives without renewal |
| `KIMISTORE_WRITER_LEASE` | `true` | Claim the whole bucket exclusively; only used when ownership is off |
| `KIMISTORE_LEASE_KEY` | `_meta/lease.json` | Object the bucket-wide claim lives at |
| `KIMISTORE_WRITER_ID` | hostname/pid | Identifies this writer in the bucket-wide claim |
| `KIMISTORE_LEASE_TTL_MS` | `60000` | How long the bucket-wide claim survives without renewal |
| `KIMISTORE_REQUIRE_LEASE` | `true` | Refuse to start if the store cannot fence writers |
| `KIMISTORE_AGENT_ID` | hostname | Stable identity that namespaces this agent's checkpoint, liveness and routing records |
| `KIMISTORE_NODE_ID` | derived from the agent id | Broker id clients see in Metadata. Stable across restarts and unique per agent; set it explicitly if a derived id could collide |
| `KIMISTORE_FLUSH_INTERVAL_MS` | `1000` | How long an `acks=all` write may wait for its segment to reach object storage; the coalescing window for PUTs and the extra ack latency |

The default configuration needs no flags to work with Grafana Mimir 3.0, and
every advertised protocol version is implemented exactly as the Kafka message
schemas declare it. A few API version ceilings are deliberate, and one of them
(Produce) is load-bearing in a way that is easy to get backwards -- see
"Version ceilings" in `COMPATIBILITY.md` before raising any of them.

### Running the Agent
```bash
# Build the agent
go build -o agent ./cmd/agent

# Run locally
./agent
```

In a container, set `KIMISTORE_ADVERTISED_HOST` to something clients can
resolve. Leaving it unset makes the agent advertise its own hostname, which is
usually right in Kubernetes and wrong everywhere else.

**Several agents per bucket are supported, one writer per partition.** Each agent claims the partitions it writes, publishes what it owns, and `Metadata` routes clients to the agent holding each partition. Consumer groups are assigned by rendezvous hashing over the same live set, and fenced so only the elected agent acts on them. Every agent needs its own advertised address. See [High availability](#high-availability).

### Testing with kcat

**Produce Messages:**
```bash
echo "hello world" | kcat -P -b localhost:19092 -t test-topic
```

**Consume (Simple):**
```bash
kcat -C -b localhost:19092 -t test-topic
```

**Consume (Consumer Group):**
```bash
# Run multiple instances in separate terminals to see load balancing
kcat -b localhost:19092 -G my-group test-topic
```

## High availability

Partitions are the unit of ownership *and* of routing: each agent claims the
partitions it writes, publishes them, and `Metadata` sends clients to the agent
holding each partition. A partition moves on crash or clean shutdown, and the
routing view drops a dead or wedged agent within the TTL.

Consumer groups are HA in the same shape. A group is coordinated by the rendezvous
winner over the live set, and every group request is fenced, so a coordinator
change is a full rebalance rather than a split brain. Consumer offsets are durable,
so a rebalance does not cost replay.

**Handover is not implemented.** A partition still only moves by crashing or by a
clean shutdown; a handover that flushes the unflushed tail before releasing is the
one remaining item.

The full design is in [`docs/ha-architecture.md`](docs/ha-architecture.md); its
§13 is a handoff with the current status, key files and where to start. As of
today:

- **Shipped:** per-partition manifests and per-agent checkpoints (phase 0);
  `acks=all` waits until its segment is in object storage (phase 1);
  per-partition ownership with per-partition epochs (phase 2); agent liveness,
  routing tables and leader-aware `Metadata` (phase 3); HA group coordination by
  rendezvous hashing with a point-of-use fence (phase 5); idempotent producers with
  duplicate-retry deduplication (phase 6).
- **Not started:** graceful handover (phase 4).
