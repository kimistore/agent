# Kimistore

**Kimistore** is a lightweight, cloud-native streaming platform built in Go. It is designed to be protocol-compatible with Apache Kafka but re-architected to run on top of commodity Object Storage (like S3) without local disks or heavy dependencies like ZooKeeper.

This project is a Apache Kafka® - compatible data streaming agent. The goal is to separate compute from storage, allowing for stateless agents that can scale instantly while durable data resides cheaply and safely in object storage.

Each agent owns one log in one bucket and holds a **writer lease** there, so scaling means giving an agent its own bucket rather than pointing several at the same one. See [Single-Writer Fence](#2b-single-writer-fence-writer-lease).

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
*   **Durability**: When a producer sets `acks=1` (or `all`), the batch is fsynced to stable storage *before* the offset is returned, so a successful ack means the data survived a crash. `acks=0` skips the flush. Concurrent appends share one flush via group commit, so parallel producers do not each pay a separate fsync.
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

### 2b. Single-Writer Fence (Writer Lease)
Two agents pointed at one bucket do not queue behind each other. They each assign offsets from their own recovered state and write segments to keys derived from those offsets, so they overwrite each other and the failure is **silent data loss** — no error, no crash, just a log that is missing records.

The agent therefore claims the log before it serves anything:

* **Claim**: A lease object (`_meta/lease.json`) inside the same bucket, acquired with a compare-and-swap (`If-None-Match: *` to create, `If-Match: <etag>` to renew). Object storage enforces both preconditions server-side, so two agents racing to start cannot both win.
* **Epoch**: The lease carries a monotonically increasing epoch, which is also stamped into the checkpoint and the manifest. A restart that finds durable state from a **newer** epoch knows it has been superseded and refuses to start, even though the lease itself was free when it looked.
* **Renewal**: A background loop renews every `TTL/3`. A renewal that fails is not immediately fatal — object stores have bad minutes — but once failures have aged past the TTL the agent stops writing, because at that point another agent may legitimately have taken the log.
* **Handover**: A graceful shutdown releases the claim, so a replacement starts immediately instead of waiting out the TTL. A crashed one blocks its replacement for at most the TTL.
* **Refusal**: Losing the lease turns into refused writes (`ErrLeaseLost`), not into a best-effort write over someone else's log.

Requires conditional-write support (all current S3-compatible stores). `KIMISTORE_REQUIRE_LEASE=false` downgrades a store without it to a loud warning; `KIMISTORE_WRITER_LEASE=false` disables the fence entirely.

### 2c. Bounded Storage Calls
Every object-store call is bounded by `KIMISTORE_S3_TIMEOUT_MS`, and the request context is threaded from the connection through the protocol handlers into the storage layer. A slow object store therefore produces a failed request the client can retry, instead of a handler goroutine that never returns. That matters more than it sounds: a hung read occupies one of the connection's in-flight slots, and once those fill, the client stops sending heartbeats and commits and the group rebalances around what is really a storage stall.

On shutdown, in-flight calls are cancelled once the final offset flush and checkpoint have landed, so a wedged store cannot hold the process open.

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
| *any other key* | — | Answered `UNSUPPORTED_VERSION`, connection preserved | ✅ |
| **CreateTopics** | 19 | Create new topics. | ✅ Active |
| **DeleteTopics** | 20 | Delete topics. | ✅ Active |

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
| `KIMISTORE_WRITER_LEASE` | `true` | Claim the log exclusively before serving |
| `KIMISTORE_LEASE_KEY` | `_meta/lease.json` | Object the claim lives at |
| `KIMISTORE_WRITER_ID` | hostname/pid | Identifies this writer in the claim |
| `KIMISTORE_LEASE_TTL_MS` | `60000` | How long a claim survives without renewal |
| `KIMISTORE_REQUIRE_LEASE` | `true` | Refuse to start if the store cannot fence writers |

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

**One agent per bucket.** The agent holds a writer lease in object storage and refuses to start when another live agent already holds it. Scale by giving each agent its own bucket, not by pointing several at one.

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
