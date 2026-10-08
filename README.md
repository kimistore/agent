# Kimistore

Kimistore is a Kafka-compatible streaming agent. The agent is written in Go.

The agent speaks the Kafka wire protocol. It stores the log in object storage.
It does not use local disks for durable data. It does not use ZooKeeper.

Each agent claims each partition that it writes. Each agent publishes its own
address. Clients route to the agent that owns each partition.

## Content

- [Design](#design)
- [Storage](#storage)
- [Durability](#durability)
- [Protocol](#protocol)
- [High availability](#high-availability)
- [Authentication](#authentication)
- [Authorization](#authorization)
- [Supported APIs](#supported-apis)
- [Run the agent](#run-the-agent)
- [Configuration](#configuration)
- [Test](#test)
- [Limits](#limits)
- [License](#license)

## Design

Kimistore separates compute from storage. The agent keeps no durable state.
The log and all control state are in the bucket.

The design has limits. Kimistore has no replication. Kimistore runs one writer
per partition. Kimistore is not a KRaft cluster.

## Storage

The agent writes each record to a local write-ahead log first. This keeps the
write path fast. The agent then uploads sealed segments to object storage.

The agent serves a read from the local log when the log holds the record. The
agent serves a read from object storage when it does not.

### Retention

Retention reclaims sealed segments. Retention is inert until you set at least
one limit.

- Set `KIMISTORE_RETENTION_MS` for the maximum segment age.
- Set `KIMISTORE_RETENTION_BYTES` for the maximum bytes per partition. Set `-1` for no limit.
- Set `KIMISTORE_RETENTION_CHECK_MS` for the sweep interval. The default is `300000`.

The agent never reclaims a segment that a consumer group still needs. The agent
keeps the newest segment because the agent cannot prove that consumers read it.

## Durability

The agent supports three durability levels. The client selects the level with
the `acks` field.

| Level | The agent does this |
| :--- | :--- |
| `acks=0` | The agent does not flush. The agent returns an offset immediately. |
| `acks=1` | The agent flushes the record to local storage. The agent returns an offset after the flush. |
| `acks=all` | The agent does not return an offset until the segment holds it in object storage. |

The agent uses posture D2 for `acks=all`. The agent withholds the offset until
object storage holds the record. An acknowledged offset is therefore always
recoverable from the bucket.

A background loop seals the active segment every `KIMISTORE_FLUSH_INTERVAL_MS`.
That interval is also the maximum extra latency that `acks=all` adds. Concurrent
producers share one flush.

The agent withholds the offset until the segment lands. The agent then advances a
durable watermark for the partition. The agent releases a producer when the
watermark covers its offset.

The watermark only moves forward across a contiguous run of segments. The agent
holds a segment that finishes early until the gap before it fills. A producer
therefore cannot be acknowledged against a hole.

The agent answers `REQUEST_TIMED_OUT` when the segment does not reach object
storage in time. The agent does not acknowledge a record that is not durable. The
record stays in the local log, so the client can retry.

### Idempotent producers

The agent implements `InitProducerId`. The agent therefore accepts batches from
an idempotent client.

The agent tracks the epoch, the next sequence, and the recent offsets per producer
and partition. The agent answers a retried batch with the offset it already has.
The agent therefore removes the duplicate that a timed-out `acks=all` retry
causes.

The agent answers `INVALID_PRODUCER_EPOCH` for a stale epoch. The agent answers
`OUT_OF_ORDER_SEQUENCE_NUMBER` for a sequence that runs ahead, unless the sequence
is merely pipelined out of order. In that case the agent waits briefly for the
gap.

The agent does not implement transactions. A transactional producer gets an id
and then fails when it opens a transaction.

## Protocol

### Single-writer fence

Two agents that write one partition overwrite each other. The agent does not
report this error. The agent loses records silently.

The agent therefore claims each partition before it writes to it.

- The claim is an object at `_owners/<topic>/<partition>`. The agent creates it with `If-None-Match: *` and renews it with `If-Match: <etag>`. Object storage checks both conditions on the server.
- The claim carries an epoch. The epoch is per partition. The agent writes the epoch into the segment key and into the manifest.
- The epoch is in the segment key because a superseded writer can finish an upload after its successor writes at the same base offset. The epoch keeps the two objects apart, so the stale one is unreachable and retention reclaims it.
- A background loop renews each claim every third of the TTL.
- A failed renewal is not fatal at first. The agent stops writing to the partition when failures age past the TTL.
- A graceful shutdown releases every claim. A replacement agent starts at once instead of waiting out the TTL.
- The agent refuses to write to an unowned partition. The agent answers `NOT_LEADER_OR_FOLLOWER`, which tells a producer to refresh its metadata.

The agent never re-acquires a claim that it cannot renew in the same process. The
partition stays fenced until a restart recovers it.

Set `KIMISTORE_PARTITION_OWNERSHIP=false` to use one bucket-wide claim instead.
The agent then refuses a second agent outright. The two modes are alternatives.
Only one fence is active at a time.

### Routing

An agent must be reachable, so each agent publishes its address and its
partitions.

- `_agents/<agent-id>/liveness` holds the broker id and the advertised address. The agent renews it with a compare-and-swap. The agent refuses two brokers that claim one identity.
- `_agents/<agent-id>/routing` lists the partitions that the agent owns. The agent writes it when the owned set changes.
- `Metadata` reports the live agents as the brokers. Each partition leader is the node id of the agent that owns the partition.
- The agent reports `LEADER_NOT_AVAILABLE` with leader `-1` for a partition whose owner has left. This makes a client refresh its metadata.
- An agent that does not own a partition serves it from object storage. Object storage is the authoritative copy.

A long-poll on a partition that this agent does not serve returns at once. The
wake-up signal fires on this process's own appends, so a park would wait for an
append that cannot happen.

### Consumer groups

The agent coordinates consumer groups.

- The agent implements `JoinGroup`, `SyncGroup`, `Heartbeat`, and `LeaveGroup`.
- The agent delegates partition assignment to the group leader.
- A background reaper evicts a member that stops sending heartbeats. The agent then advances the generation and re-elects a leader.
- A `SyncGroup` follower waits for the leader with a bounded timeout. A dead leader therefore cannot pin a follower.
- `OffsetCommit` is answered at once. The agent flushes it to object storage on a background loop. The agent retries a failed flush.
- `FindCoordinator` selects the coordinator by rendezvous hashing over the live set.
- `JoinGroup`, `SyncGroup`, and `Heartbeat` check that this agent is the coordinator. The agent answers `NOT_COORDINATOR` otherwise.
- The group state is in memory. A coordinator change therefore causes a full rebalance. Consumer offsets are in object storage, so a consumer resumes where it stopped.

### Bounded storage calls

Every object-store call has the `KIMISTORE_S3_TIMEOUT_MS` deadline. A slow store
therefore fails a request that the client can retry. A hung read would otherwise
occupy an in-flight slot until the client stops sending heartbeats.

### Durable metadata layout

The log position is per partition. The checkpoint is per agent.

- `_topics/<topic>/_manifest/<partition>` holds the log end offset, the log start offset, and the segment inventory for one partition. The agent stamps it with the ownership epoch.
- `_agents/<agent-id>/checkpoint.json` holds the committed offsets and the coordinator state.
- A bucket that an older agent wrote keeps its single `_meta/checkpoint.json`. The agent reads it once on upgrade.

## High availability

Each agent claims the partitions that it writes. Each agent publishes them.
`Metadata` sends each client to the agent that holds the partition. A partition
moves when its owner crashes or shuts down. The routing view drops a dead agent
within the TTL.

A group is coordinated by the rendezvous winner over the live set. Every group
request is fenced, so a coordinator change is a full rebalance rather than a
split brain. Consumer offsets are durable, so a rebalance does not cost a replay.

Each agent needs its own advertised address. Clients dial that address, so a
wrong `KIMISTORE_ADVERTISED_HOST` moves them to a broken broker.

See [`docs/ha-architecture.md`](docs/ha-architecture.md) for the full design.

## Authentication

The agent supports SASL. SASL/PLAIN and SCRAM-SHA-256 and SCRAM-SHA-512 are
available.

The agent does not require authentication unless you configure it. The agent
advertises only the mechanisms that it can complete.

### SASL/PLAIN

Set `SASL_USERNAME` and `SASL_PASSWORD` to require PLAIN. All clients then share
one credential. PLAIN sends the password on the wire, so use it only over TLS.

### SCRAM

SCRAM does not send the password. The client proves that it knows the password.

The agent stores the credential in the bucket under `_scram/`. Each agent that
points at the bucket therefore offers the same credentials. You rotate a
credential with one object write.

Create a credential:

```bash
printf 'the-password' | kimistore-credential create -user alice -password-stdin
```

Add a second mechanism for the same user:

```bash
printf 'the-password' | kimistore-credential create -user alice -password-stdin -mechanism SCRAM-SHA-512
```

List the credentials:

```bash
kimistore-credential list
```

The command reads the password from standard input. The command never accepts a
password as an argument, because arguments appear in the process list.

The command never opens a local write-ahead log. It writes one prefix of one
bucket.

An existing connection stays authenticated after a rotation. The agent checks a
credential once per connection.

## Authorization

The agent can restrict what each principal may do on each topic.

With no rules, the agent allows every request. An upgrade therefore changes
nothing.

As soon as one rule exists, the agent denies by default. A rule with a wrong topic
name therefore locks data out instead of exposing data. A deny rule overrides an
allow rule, whatever the order.

Add a rule:

```bash
kimistore-credential acl add -principal alice -operation Read -topic orders
```

Grant a producer the permissions that it needs:

```bash
kimistore-credential acl add -principal alice -operation Describe -topic orders
kimistore-credential acl add -principal alice -operation Write -topic orders
```

A producer needs `Describe` before it can write. A grant of `Write` without
`Describe` therefore fails at metadata time.

List the rules:

```bash
kimistore-credential acl list
```

Remove a rule:

```bash
kimistore-credential acl remove -principal alice -operation Read -topic orders
```

The operations are `Read`, `Write`, `Create`, `Delete`, `Describe`, `Alter`, and
`All`. Use `-principal *` for every caller. Leave `-topic` empty for every topic.
Use `-group` for a consumer group instead of a topic.

The agent stores the rules under `_acl/`. The agent reads them at startup. The
agent then refreshes them in the background. The request path never contacts the
object store.

The agent keeps the previous rules when a refresh fails. The agent therefore does
not allow every request because the bucket was briefly unreachable.

## Supported APIs

The agent implements the following APIs.

| API | Key | Versions | Purpose |
| --- | --- | --- | --- |
| `Produce` | 0 | V0-V3 | Send records. |
| `Fetch` | 1 | V0-V5 | Read records. |
| `ListOffsets` | 2 | V0-V2 | Get the earliest and latest offsets. |
| `Metadata` | 3 | V0-V7 | Discover brokers and topics. |
| `OffsetCommit` | 8 | V0 | Store consumer offsets. |
| `OffsetFetch` | 9 | V0-V1 | Read consumer offsets. |
| `FindCoordinator` | 10 | V0 | Locate the group coordinator. |
| `JoinGroup` | 11 | V0-V1 | Register a group member. |
| `Heartbeat` | 12 | V0 | Keep the member session alive. |
| `LeaveGroup` | 13 | V0 | Leave the group. |
| `SyncGroup` | 14 | V0 | Distribute assignments. |
| `DescribeGroups` | 15 | V0 | Describe a group. |
| `ListGroups` | 16 | V0 | List the groups. |
| `SaslHandshake` | 17 | V0-V1 | Select a SASL mechanism. |
| `ApiVersions` | 18 | V0 | Negotiate the protocol. |
| `CreateTopics` | 19 | V0 | Create topics. |
| `DeleteTopics` | 20 | V0 | Delete topics. |
| `InitProducerId` | 22 | V0-V1 | Allocate a producer id. |
| `SaslAuthenticate` | 36 | V0-V1 | Run the SASL exchange. |

The rows sort by API key. The `SaslAuthenticate` key is 36, so the row sits at the
end of the table.

The agent answers `UNSUPPORTED_VERSION` for any other key. The agent keeps the
connection open.

See "Version ceilings" in [`COMPATIBILITY.md`](COMPATIBILITY.md) before you raise
a ceiling. The `Produce` ceiling is load-bearing.

## Run the agent

You need Go 1.27 or later. You need an S3-compatible bucket.

Build the agent:

```bash
go build -o agent ./cmd/agent
```

Run the agent:

```bash
./agent
```

In a container, set `KIMISTORE_ADVERTISED_HOST` to an address that the client can
resolve. An unset value makes the agent advertise its own hostname. That is
usually correct in Kubernetes and wrong elsewhere.

Produce a record:

```bash
echo "hello world" | kcat -P -b localhost:19092 -t test-topic
```

Read the records:

```bash
kcat -C -b localhost:19092 -t test-topic
```

Read the records as a group:

```bash
kcat -b localhost:19092 -G my-group test-topic
```

## Configuration

The agent reads these environment variables.

| Variable | Default | Purpose |
| :--- | :--- | :--- |
| `KIMISTORE_LISTEN_ADDR` | `:19092` | Address for the broker socket. |
| `KIMISTORE_ADVERTISED_HOST` | Hostname | Address that the client dials. This address must be reachable. |
| `KIMISTORE_ADVERTISED_PORT` | Port of the listen address | Address that the client dials. |
| `KIMISTORE_METRICS_ADDR` | `:9091` | Prometheus endpoint. |
| `KIMISTORE_WAL_DIR` | `./data/wal` | Local write-ahead log. A shutdown seals and uploads every segment, so the agent can lose this directory. |
| `S3_BUCKET` | `kimistore` | Bucket name. |
| `AWS_REGION` | `us-east-1` | Bucket region. |
| `S3_ENDPOINT` | Unset | Custom endpoint for a compatible store. |
| `SASL_USERNAME` | Unset | Enables SASL/PLAIN. |
| `SASL_PASSWORD` | Unset | Password for SASL/PLAIN. |
| `KIMISTORE_RETENTION_MS` | `0` | Maximum segment age. Zero disables retention. |
| `KIMISTORE_RETENTION_BYTES` | `-1` | Maximum bytes per partition. `-1` means no limit. |
| `KIMISTORE_RETENTION_CHECK_MS` | `300000` | Retention sweep interval. |
| `KIMISTORE_S3_TIMEOUT_MS` | `30000` | Deadline for one object-store request. |
| `KIMISTORE_PARTITION_OWNERSHIP` | `true` | Claim each partition separately. Set false for one bucket-wide claim. |
| `KIMISTORE_OWNERSHIP_TTL_MS` | `60000` | Time that a partition claim survives without renewal. |
| `KIMISTORE_WRITER_LEASE` | `true` | Take the bucket-wide claim. This variable applies only when ownership is off. |
| `KIMISTORE_LEASE_KEY` | `_meta/lease.json` | Object for the bucket-wide claim. |
| `KIMISTORE_WRITER_ID` | Hostname and pid | Identifies this writer in the bucket-wide claim. |
| `KIMISTORE_LEASE_TTL_MS` | `60000` | Time that the bucket-wide claim survives without renewal. |
| `KIMISTORE_REQUIRE_LEASE` | `true` | Refuse to start when the store cannot fence writers. |
| `KIMISTORE_AGENT_ID` | Hostname | Identity that namespaces the checkpoint, liveness, and routing records. |
| `KIMISTORE_NODE_ID` | Derived from the agent id | Broker id that the client sees. Set this value explicitly when a derived id can collide. |
| `KIMISTORE_FLUSH_INTERVAL_MS` | `1000` | Time that an `acks=all` write waits for object storage. This interval also coalesces the uploads. |

The default configuration works with Grafana Mimir 3.0 without any variable.

## Test

Run the unit tests:

```bash
make test
```

Run the unit tests under the race detector:

```bash
make race
```

Show the coverage:

```bash
make cover
```

Run the linter:

```bash
make lint
```

Run everything that the continuous integration runs:

```bash
make ci
```

Run the Mimir end-to-end test:

```bash
make e2e
```

The end-to-end test starts a real Grafana Mimir. The agent is the Kafka landing
zone for it. The test pushes remote-write samples. The test reads them back with
a PromQL query. The test then restarts the agent with an empty local
write-ahead log, so it proves that the data survives. The test starts its own
local S3 shim, so you need only Docker.

Mimir 3.x uses `twmb/franz-go`. The rest of the test suite uses
`segmentio/kafka-go`. The end-to-end test is therefore the check that matters for
protocol compatibility.

## Limits

The agent has the following limits.

- The agent has no replication. A failover costs a degraded read path until a peer claims the partition.
- The claim time to live is the failover window.
- A `SIGTERM` drains the partitions first. The agent seals each segment, waits for object storage, writes the manifest, and releases the claim in that order. A partition whose tail is not durable keeps its claim. It expires on its own schedule, which is a slower failover rather than a lossy one. Set the container grace period above 25 seconds, which is the `DrainShutdownBudget`.
- A `SIGKILL` drains nothing. The agent releases its claims in bulk and the next owner reconciles its position from object storage. Set the ownership time to live as low as the object store tolerates, because it sets the crash failover window.
- The write path trusts a local view of the claim. An agent that loses the claim keeps accepting writes until its next renewal, up to a third of the claim time to live. Durable state it writes in that window is fenced against the object store, so it cannot corrupt the new owner.
- The agent acknowledges a consumer offset before it stores the offset.
- The agent does not fence `OffsetCommit` and `OffsetFetch` to the group coordinator.
- Idempotent producer state does not survive a move to another machine.
- The agent does not enforce `Describe` on consumer groups.
- SASL/PLAIN shares one credential between all clients.
- The agent does not support topic-level ACL patterns such as `orders-*`.

See §14 of [`docs/ha-architecture.md`](docs/ha-architecture.md) for the current
status of these limits.

## License

Copyright (C) 2026 Andy Lo-A-Foe.

The agent is licensed under the GNU Affero General Public License version 3 or
later. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).

The broker serves clients over a network. Section 13 of the license therefore
applies. Anyone who uses the agent over a network can get the source.
