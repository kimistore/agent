# Kimistore

**Kimistore** is a lightweight, cloud-native streaming platform built in Go. It is designed to be protocol-compatible with Apache Kafka but re-architected to run on top of commodity Object Storage (like S3) without local disks or heavy dependencies like ZooKeeper.

This project is a Apache Kafka® - compatible data streaming agent. The goal is to separate compute from storage, allowing for stateless agents that can scale instantly while durable data resides cheaply and safely in object storage.

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
*   **Consuming**: Supports fetching messages with offset management, including **long polling**. When a consumer is caught up and asks for data (`minBytes > 0`), the request parks on an append signal for up to `maxWaitMs` (capped at 1s) instead of returning empty immediately. This stops caught-up consumers from re-polling in a tight loop.
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

## 📡 Supported Kafka APIs

The following Kafka API Keys are currently implemented:

| API Name | API Key | Description | Status |
| :--- | :---: | :--- | :--- |
| **Produce** | 0 | Send messages to the broker. | ✅ Active (V0-V2) |
| **Fetch** | 1 | Consume messages from valid offsets. | ✅ Active (V0-V2) |
| **ListOffsets** | 2 | Get earliest/latest (HighwaterMark) offsets. | ✅ Active |
| **Metadata** | 3 | Discover brokers, topics, and dynamic partitions. | ✅ Active |
| **OffsetCommit** | 8 | Save consumer group offsets to Object Store. | ✅ Active (S3) |
| **OffsetFetch** | 9 | Retrieve consumer group offsets from Object Store. | ✅ Active (S3) |
| **FindCoordinator** | 10 | Locate the group coordinator. | ✅ Active |
| **JoinGroup** | 11 | Register a consumer member. | ✅ Active (V0-V1) |
| **Heartbeat** | 12 | Keep member session alive. | ✅ Active |
| **LeaveGroup** | 13 | Graceful consumer shutdown. | ✅ Active |
| **SyncGroup** | 14 | Distribute partition assignments. | ✅ Active |
| **DescribeGroups** | 15 | Get detailed group/member info. | ✅ Active |
| **ListGroups** | 16 | List active consumer groups. | ✅ Active |
| **ApiVersions** | 18 | Negotiate protocol support. | ✅ Active |
| **CreateTopics** | 19 | Create new topics. | ✅ Active |
| **DeleteTopics** | 20 | Delete topics. | ✅ Active |

## 🛠 Usage

### Prerequisites
*   Go 1.27+
*   S3-compatible bucket (or local filesystem simulation)
*   `kcat` (recommended for testing)

### Running the Agent
```bash
# Build the agent
go build -o agent cmd/agent/main.go

# Run locally (defaults to :19092, using local storage dir)
./agent
```

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
