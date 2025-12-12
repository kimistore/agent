
## Compatibility Guide

This server implements a subset of the Kafka Protocol (primarily V0-V2). It is designed to be compatible with standard Kafka clients, but strictly for **Simple Producer** and **Simple Consumer** workflows.

### Supported Features
*   **Protocol Versions**: Kafka 0.10.x era (ApiVersions V0, Produce V2, Fetch V2, Metadata V2).
*   **Message Format**: MessageSet V0/V1 (Legacy).
    *   *Note*: Modern clients sending RecordBatch V2 (Kafka 0.11+) will be treated as opaque batches. They will succeed, but offset increments will be 1 per batch rather than 1 per message.
*   **Discovery**: `Metadata` requests work for topic/partition discovery.
*   **Data Persistence**: Full S3 offloading supported.

### Limitations (What WON'T work)
1.  **Consumer Groups**: Features like `group.id`, auto-balancing, and offset committing (`FindCoordinator`, `JoinGroup`, etc.) are **NOT implemented**.
    *   *Result*: Clients trying to use consumer groups will likely hang or error waiting for a coordinator.
    *   *Fix*: You must manually assign partitions (e.g., `assign([TopicPartition(topic, 0)])` instead of `subscribe([topic])`).
2.  **Transactions/Idempotency**: `InitProducerId` is not supported.
3.  **Modern RecordBatch**: Parsing individual records inside a V2 RecordBatch is not yet implemented.

### Client Configuration Examples

#### Python (confluent-kafka / librdkafka)
```python
conf = {
    'bootstrap.servers': 'localhost:19092',
    # 'group.id': 'my-group', # DO NOT USE GROUPS
    'api.version.request': True,
}
c = Consumer(conf)
# Do NOT use c.subscribe(). Use c.assign()
c.assign([TopicPartition('my-topic', 0)])
```

#### Java (KafkaClient)
```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:19092");
// disable auto commit/groups if possible or just use assign()
// props.put("enable.auto.commit", "false");

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
consumer.assign(Arrays.asList(new TopicPartition("my-topic", 0)));
```

#### CLI (kcat)
```bash
# Producer works automatically (negotiates version)
kcat -b localhost:19092 -t my-topic -P

# Consumer works using simple consumer mode (-C is actually simple consumer by default in kcat, but acts like group if -G is passed)
kcat -b localhost:19092 -t my-topic -C -o beginning
```
