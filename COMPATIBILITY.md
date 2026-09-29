## Compatibility Guide

This server implements a subset of the Kafka Protocol (primarily V0-V2). It is designed to be compatible with standard Kafka clients and supports both **Simple Consumer** and **Consumer Group** workflows.

### Supported Features
*   **Protocol Versions**: Kafka 0.10.x era (ApiVersions V0, Produce V2, Fetch V2, Metadata V2, JoinGroup V1).
*   **Message Format**: 
    *   MessageSet V0/V1 (Legacy).
    *   RecordBatch V2 (Kafka 0.11+). Record counts are correctly parsed from RecordBatch headers to ensure accurate offset tracking (one per record, rather than one per batch).
*   **Decompression**: Automatic decompression of GZIP, Snappy, and LZ4 compressed message batches for accurate offset tracking.
*   **Durability**: Acknowledged writes are fsynced before the producer is
    given its offset. This follows Kafka's `acks` semantics:

    | `acks` | Behaviour |
    | :--- | :--- |
    | `0` | No response expected. The batch is written to the WAL but **not** fsynced. Fastest; a crash may lose it. |
    | `1` | Batch is fsynced before the offset is returned. A successful ack means the data survived a crash. |
    | `-1` | Same as `1`. This agent is single-node, so it is its own only replica. |

    Concurrent appends share a single flush (group commit), so many producers
    writing in parallel do not each pay for their own fsync. A single
    sequential producer still costs roughly one device flush per batch, which
    is the floor for any `acks=1` broker.
*   **Consumer Groups**: Features a built-in "Lite" Group Coordinator supporting:
    *   Dynamic partition assignment and load balancing (`JoinGroup`, `SyncGroup`, `LeaveGroup`).
    *   Background session/heartbeat tracking (`Heartbeat`).
    *   Persistent offset commits saved cheaply to object storage (`OffsetCommit`, `OffsetFetch`).
*   **Discovery**: `Metadata` and `FindCoordinator` requests allow clients to auto-discover brokers, topics, partitions, and group coordinators.
*   **Data Persistence**: Low-latency local Write-Ahead Log (WAL) with transparent asynchronous segment offloading to AWS S3 or compatible object stores.

### Limitations (What WON'T work)
1.  **Transactions/Idempotency**: Transactional producing (`InitProducerId`, transaction markers) is not supported.
2.  **Replication**: Multi-broker data replication (partition leaders and ISRs) is not implemented. The agent operates as a single-node stateless caching proxy/broker.

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
| **Core Protocol** | Produce API (V2) | ✅ Supported | Batch duration/parsing pending |
| | Fetch API (V2) | ✅ Supported | |
| | ListOffsets (V1) | ✅ Supported | |
| | Metadata (V2) | ✅ Supported | |
| | ApiVersions (V0) | ✅ Supported | Higher versions return Error |
| **Messaging** | MessageSets (V0, V1) | ✅ Supported | Legacy format |
| | RecordBatch (V2) | ✅ Supported | record count parsed correctly; offset tracking is accurate |
| | Compression | ✅ Supported | GZIP, Snappy, and LZ4 decompressed for counting |
| **Consumption** | Simple Consumer | ✅ Supported | `assign()` partitions manually |
| | Consumer Groups | ✅ Supported | `subscribe()` is fully operational via Lite Coordinator |
| | Offset Commit | ✅ Supported | Group progress committed/loaded from S3 |
| **Durability** | Local Persistence | ✅ Supported | Synchronous WAL |
| | Acked-write fsync | ✅ Supported | Group-committed; honours `acks` |
| | S3 Offload | ✅ Supported | Asynchronous upload |
| | S3 Recovery | ✅ Supported | Transparent fallback to S3 |
| **Reliability** | Replication | ❌ No | Single node only |
| | ISR/HW | ❌ No | Always ISR=1, HW=Max |
