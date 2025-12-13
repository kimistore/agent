# Consumer Groups Implementation Plan

## 1. Concept: What are Consumer Groups?
In Kafka, a **Consumer Group** is a mechanism to allow a pool of consumers to divide up the work of processing a topic's partitions.
*   **Load Balancing**: Partitions are automatically assigned to members of the group. If a member joins or leaves, partitions are rebalanced.
*   **Offset Tracking**: The group stores its progress (offsets) on the broker (specifically in the `__consumer_offsets` topic). This allows consumers to restart without losing their place.

## 2. Required Protocol APIs
To support consumer groups, we must implement the **Group Coordinator** protocol.

### Phase 1: Coordination Layer (The "Hard" Part)
This layer handles the lifecycle of the group.
1.  **FindCoordinator (API Key 10)**: Clients ask "Who manages group X?". We must return our broker ID.
2.  **JoinGroup (API Key 11)**: Consumers request to join. The broker must:
    *   Elect a Leader Consumer.
    *   Collect metadata (subscriptions) from all members.
    *   Block until `session.timeout.ms` or all members join.
3.  **SyncGroup (API Key 14)**: The Leader Consumer calculates assignments and sends them back to the broker. The broker distributes them to all members.
4.  **Heartbeat (API Key 12)**: Members keep their session alive.
5.  **LeaveGroup (API Key 13)**: Graceful exit.

### Phase 2: Offset Management
Once the group is running, they need to save state.
1.  **OffsetCommit (API Key 8)**: Save offset for [Topic, Partition].
    *   *Implementation*: We need a KV store or a special internal topic (`__consumer_offsets`) to persist this.
2.  **OffsetFetch (API Key 9)**: Retrieve saved offsets on startup.

## 3. Implementation Strategy for "Go-Stream"

### Option A: The "Lite" Static Coordinator (MVP)
*   **Logic**: Assume a single-node cluster. This node IS the coordinator for all groups.
*   **Storage**: Store group metadata and offsets in memory (map) initially, backed by a simple file (JSON/GOB) or a special WAL partition for durability.
*   **Rebalancing**: Implement a simplified rebalance state machine.
    *   *State "PreparingRebalance"*: Wait for JoinGroup requests.
    *   *State "CompletingRebalance"*: Wait for SyncGroup.
    *   *State "Stable"*: Accept Heartbeats/Commits.
*   **Feasibility**: **High**. Complex, but solvable in ~1-2 weeks of focused work.

### Option B: Full Distributed Coordinator (Hard)
*   **Logic**: Full hashing ring to distribute groups across multiple agents. Requires consensus (Etcd/Zookeeper/Gossip).
*   **Feasibility**: **Low** for this current single-binary project structure. Overkill.

## 4. Proposed Logical Architecture (Single Node)

```go
type GroupCoordinator struct {
    groups map[string]*GroupMetadata
    mu     sync.Mutex
}

type GroupMetadata struct {
    state       string // Empty, PreparingRebalance, Stable
    members     map[string]*MemberMetadata
    offsets     map[TopicPartition]int64
    generation  int
}
```

### Key Challenges
1.  **Blocking Requests**: `JoinGroup` and `SyncGroup` are blocking calls (Long Polling). We need to handle Go channels/timers effectively to hold the HTTP connection open until the group stabilizes.
2.  **State Machine Correctness**: Getting the rebalance state machine wrong causes "Stop the World" looping where consumers constantly rejoin.

## 5. Feasibility Rating
*   **Complexity**: 8/10
*   **Effort**: High
*   **Value**: Critical for "Real World" usage. Without it, you cannot run standard microservices (e.g. Spring Boot Kafka, standard K8s deployments).

## 6. Next Steps (If proceeding)
1.  Implement `FindCoordinator` (Trivial: always return self).
2.  Implement `OffsetCommit` / `OffsetFetch` invalidating the group requirement first (allow "Simple consumer with offset storage").
3.  Tackle `JoinGroup` / `SyncGroup` state machine.
