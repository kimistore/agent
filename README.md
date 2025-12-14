# Go-Stream

**Go-Stream** is a lightweight, cloud-native streaming platform built in Go. It is designed to be protocol-compatible with Apache Kafka but re-architected to run on top of commodity Object Storage (like S3) without local disks or heavy dependencies like ZooKeeper.

This project is an educational re-implementation inspired by the architecture of [WarpStream](https://www.warpstream.com/). The goal is to separate compute from storage, allowing for stateless agents that can scale instantly while durable data resides cheaply and safely in object storage.

## 🚀 Project Goal

The primary objective is to build a "serverless" Kafka-compatible broker that:
1.  **Speaks Kafka Protocol**: Works with standard Kafka clients (kcat, Java, Python, Go consumers/producers).
2.  **uses Object Storage**: Persists all data to S3 (or compatible stores) instead of local disks (`EBS`).
3.  **Stateless Architecture**: Agents acts as a proxy/cache layer. State is in the object store.

## ✅ Implemented Features

As of the current version, the following core components are working:

### 1. Storage Layer (S3-Backed WAL)
*   **Write-Ahead Log (WAL)**: Records are locally appended to partitioned WAL files for low-latency writes.
*   **Asynchronous Uploads**: Background routines automatically roll segments (e.g., at 1MB or time intervals) and upload them to the configured Object Store.
*   **Unified Read Path**: Consumers read from the "Hot" WAL (RAM/Local Disk) for real-time tailing and seamlessly fallback to "Cold" Object Storage for historical reads.
*   **High Watermark**: Correct handling of log end offsets to ensure consumers waiting for data receive empty success responses rather than errors.

### 2. Kafka Protocol Support
We implement a subset of the Kafka binary protocol sufficient to support standard producers and consumers.

*   **Producing**: Supports batch production of messages to specific topics and partitions.
*   **Consuming**: Supports fetching messages with offset management.
*   **Dynamic Partitions**: Topics and partitions are auto-discovered from the underlying storage layout.

### 3. Consumer Groups (Lite Coordinator)
A built-in "Lite" Group Coordinator allows multiple consumers to work together to consume topics:
*   **Membership Management**: Handles `JoinGroup`, `LeaveGroup`, and `Heartbeat` life-cycles.
*   **Leader-Based Assignment**: Delegates partition assignment to the consumer group leader (standard Kafka client behavior).
*   **Rebalancing**: Supports adding/removing consumers dynamically with minimal disruption.
*   **Partition Awareness**: Correctly distributes partitions (e.g., 2 partitions -> 2 consumers) across the group.

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
| **JoinGroup** | 11 | Register a consumer member. | ✅ Active |
| **Heartbeat** | 12 | Keep member session alive. | ✅ Active |
| **LeaveGroup** | 13 | Graceful consumer shutdown. | ✅ Active |
| **SyncGroup** | 14 | Distribute partition assignments. | ✅ Active |
| **ApiVersions** | 18 | Negotiate protocol support. | ✅ Active |
| **CreateTopics** | 19 | Create new topics. | ✅ Active |
| **DeleteTopics** | 20 | Delete topics. | ✅ Active |

## 🛠 Usage

### Prerequisites
*   Go 1.22+
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
