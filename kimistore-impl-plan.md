# kimistore-impl-plan.md

## Overview

This document outlines a comprehensive plan to implement **Kimistore**, a Kafka-compatible streaming platform in **Go**, utilizing **Object Storage (S3)** as the primary storage backend. 

Per the requirements, this implementation will adopt the **AutoMQ storage architecture**, specifically the **S3WAL** concept, to achieve low-latency writes while leveraging the cost-effectiveness and scalability of object storage.

## Architecture

The system will consist of three main layers:

1.  **Protocol Layer (Frontend)**: Handles Kafka Wire Protocol connections, request parsing, and response serialization.
2.  **Metadata Layer (Control Plane)**: Manages cluster state, topic configurations, partition assignments, and consumer group offsets.
3.  **Storage Layer (S3WAL + Object Store)**: The core engine that handles data durability, implementing a tiered storage approach inspired by AutoMQ.

### High-Level Diagram

```mermaid
graph TD
    Client[Kafka Client] -->|Kafka Protocol| Agent[Stateless Agent (Go)]
    
    subgraph "Agent"
        Protocol[Protocol Handler]
        Coord[Coordinator / State Manager]
        Storage[Storage Engine]
    end
    
    Agent -->|Metadata Ops| MetaStore[Metadata Store (etcd/embedded)]
    
    Storage -->|1. Append & Ack| WAL[Local WAL (EBS/Disk)]
    Storage -.->|2. Async Upload| S3[Object Storage (S3/GCS)]
    
    WAL -->|Read Hot Data| Storage
    S3 -->|Read Cold Data| Storage
```

---

## 1. Storage Layer Design (AutoMQ Approach)

The key differentiator is the usage of an **S3WAL (Write Available Log)**. Instead of writing directly to S3 (which has high latency) or storing everything on local disks (expensive, hard to scale), we use a hybrid approach.

### 1.1 The "S3Stream" Abstraction
We will treat each Topic-Partition as a continuous **Stream** of bytes.
-   The stream is broken down into **Segments**.
-   **WAL (Hot Store)**: The active segment is being written to a local high-performance disk (or EBS volume).
-   **Object Store (Cold Store)**: Sealed segments are uploaded to S3.

### 1.2 Write Path (Producer)
1.  **Receive**: Agent receives `ProduceRequest`.
2.  **Append**: Data is appended to the local **WAL** file.
    -   *Crucial*: Perform a `fsync` (or rely on Direct I/O) to guarantee durability on the local block device.
3.  **Ack**: Once persisted to WAL, respond with `ProduceResponse` (OK).
    -   *Result*: Low latency (milliseconds) comparable to standard Kafka.
4.  **Upload (Optimization)**: A background goroutine monitors the WAL.
    -   When a segment reaches a size threshold (e.g., 64MB) or time limit, it is **sealed**.
    -   The sealed segment is uploaded to Object Storage.
5.  **Trim**: Once safely in S3, the local WAL segment can be truncated/deleted to free space.

### 1.3 Read Path (Consumer)
1.  **Receive**: Agent receives `FetchRequest` for a specific Offset.
2.  **Locate**: Determine if the offset is in the **WAL** (Hot) or **S3** (Cold).
    -   *Hot Read*: If the offset is recent, read directly from the local WAL file (very fast).
    -   *Cold Read*: If the offset is older, download the relevant segment range from S3 (high throughput).
3.  **Response**: Send `FetchResponse` with record batches.

---

## 2. Supported Kafka APIs

We will target the **15 Core APIs** necessary for a functional streaming platform.

| Priority | API Name | Description | Status |
| :--- | :--- | :--- | :--- |
| **Critical** | **Produce** | Writing messages to topics. | P0 |
| **Critical** | **Fetch** | Reading messages from topics. | P0 |
| **Critical** | **Metadata** | Discovering brokers, topics, and partitions. | P0 |
| **Critical** | **ApiVersions** | Negotiating protocol versions. | P0 |
| **High** | **ListOffsets** | Finding start/end offsets for partitions. | P1 |
| **High** | **OffsetCommit** | Saving consumer group progress. | P1 |
| **High** | **OffsetFetch** | Retrieving consumer group progress. | P1 |
| **High** | **FindCoordinator**| Locating the coordinator for a group. | P1 |
| **High** | **JoinGroup** | Consumer group membership. | P1 |
| **High** | **SyncGroup** | Consumer group state synchronization. | P1 |
| **High** | **Heartbeat** | Maintaining consumer group liveness. | P1 |
| **Medium** | **CreateTopics** | Admin API to create topics. | P2 |
| **Medium** | **DeleteTopics** | Admin API to delete topics. | P2 |
| **Low** | **OffsetDelete** | Removing offsets. | P3 |
| **Low** | **InitProducerID** | Idempotent producer support. | P3 |

---

## 3. Implementation Plan (Golang)

### Phase 1: Foundation & Protocol (Weeks 1-2) - [DONE]
*   **Goal**: Establish a TCP server that speaks Kafka.
*   **Tech**: Go `net` package, `github.com/segmentio/kafka-go` (for protocol structures) or custom decoding if needed for fine-grained control.
*   **Deliverable**: A server that accepts connections and responds to `ApiVersions` and `Metadata` requests.

### Phase 2: The Storage Engine (S3WAL) (Weeks 3-4) - [DONE]
*   **Goal**: Implement the WAL writing and S3 uploading.
*   **WAL Implementation**:
    -   Create a directory structure: `/data/wal/<topic>/<partition>/`.
    -   Implement a continuous appender using `os.File` with `O_APPEND`.
    -   Implement an in-memory index mapping `Offset -> <File, Position>`.
*   **S3 Uploader**:
    -   Use `aws-sdk-go-v2`.
    -   Background worker that watches WAL files.
    -   Uploads strictly ordered segments to S3 key path: `s3://<bucket>/<topic>/<partition>/<start_offset>.log`.
*   **Deliverable**: Internal Go API to `Append(topic, partition, batch)` and `Read(topic, partition, offset)`.

### Phase 3: Producer & Consumer (Protocol Layer) (Week 5) - [DONE]
*   **Goal**: Connect the Network Layer (Phase 1) with the Storage Engine (Phase 2).
*   **Handler Implementation**:
    -   Update `HandleRequest` to parse `Produce` and `Fetch` requests.
    -   **Produce**: Parse batches -> `storage.Append()`.
    -   **Fetch**: Parse offset -> `storage.Read()`.
*   **Deliverable**: A working "echo" system where you can Produce to it and Fetch from it using a standard Kafka client (`kcat` or `kafka-console-producer`).
    -   Handle "Not Found" or "Offset Out of Range".
*   **Deliverable**: Can produce messages via `kcat` and consume them back.

### Phase 4: S3 Reads & Recovery (Week 6) - [DONE]
*   **Goal**: Ensure reading works for data offloaded to S3.
*   **S3 Read Path**:
    -   Update `StorageEngine.Read`: If not in hot WAL, check S3.
    -   Need a strategy to find *which* S3 key has the offset. (Naive listing or Index).
    -   For MVP: simple `ListObjects` filtering for range.
*   **Recovery**:
    -   Agent startup should be able to resume appending. (Already tackled in Phase 2 via WAL recovery).
*   **Deliverable**: Test creating a large backlog, letting it upload, then consume it from start.

---

## 4. Key Technical Decisions

1.  **Language**: Go (Golang) 1.21+.
2.  **Concurrency Model**:
    -   One Goroutine per TCP connection (client).
    -   Worker pools for disk I/O to avoid blocking protocol parsers.
3.  **Object Storage Interface**:
    -   Abstract interface `ObjectStore` with methods `Put`, `Get`, `Range`.
    -   Implementations for `S3` (AWS), `GCS` (Google), and `File` (Local, for testing).
4.  **Zero-Copy Optimization**:
    -   For `Fetch` requests reading from the WAL, try to use `sendfile` (via `io.Copy` from file to socket) where possible to minimize userspace copying.

## 5. Next Steps

To begin immediately, I recommend starting with **Phase 1 & 2** concurrently:
1.  **Scaffold the project**: `go mod init kimistore`.
2.  **Define the Storage Interface**: Create the Go interfaces for the WAL and Object Store.
3.  **Prototypes**: Write a simple "Append-Only Log" that offloads to MinIO (S3 compatible) in the background.

## 6. Known Issues / Limitations

### Batch vs Message Offsets - [RESOLVED]
*   **Resolution**: The `StorageEngine` and `WAL` interfaces were updated to accept `recordCount`. The `handleProduce` function now parses the incoming `MessageSet` (V0/V1) to count records and increments the offset accordingly.
*   **Implication**: Offsets now correctly reflect the number of messages in the batch.
*   **Note**: Compressed batches are treated as 1 message for now (offset +1), which aligns with how Wrapper messages work in V0/V1 if we don't perform deep inspection. For uncompressed batches, offsets are accurate.
