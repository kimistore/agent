# Changelog

All notable changes to Kimistore are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[semantic versioning](https://semver.org/spec/v2.0.0.html).

A version is `MAJOR.MINOR.PATCH`. The project is pre-1.0, so the minor number
carries the breaking changes and the patch number carries the fixes.

## Unreleased

Nothing yet.

## 1.0.0

The first release. Kimistore serves the Kafka protocol from a Write-Ahead Log
on local disk with segments in object storage, so a single node keeps the
Kafka client contract while the durable data lives in S3 or a compatible
store.

### Added

- Produce, Fetch, ListOffsets, Metadata, OffsetCommit, OffsetFetch,
  FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, DescribeGroups,
  ListGroups, ApiVersions, CreateTopics, DeleteTopics, and InitProducerId
- Both record encodings: the bare RecordBatch that `segmentio/kafka-go` writes
  and the message-set-wrapped form that the Java client, librdkafka, and
  sarama write. The record count is read from the batch header, so the log end
  offset advances by one per record rather than one per batch
- GZIP, Snappy, and LZ4 decompression for accurate offset tracking
- Kafka `acks` semantics, with `acks=all` withholding the offset until the
  segment holding it is in object storage
- Idempotent producers through InitProducerId, with retried batches answered
  with their original offset
- A group coordinator with dynamic assignment, heartbeat tracking, and offset
  commits stored in object storage
- SASL/SCRAM-SHA-256 and SASL-SHA-512, with credentials under `_scram/`. The
  server chooses the iteration count and stores it, so a client cannot
  downgrade the work factor
- SASL/PLAIN, configured with `SASL_USERNAME` and `SASL_PASSWORD`, for one
  shared credential
- Topic authorization on Produce, Fetch, Metadata, CreateTopics, and
  DeleteTopics, with rules under `_acl/`
- Segment offloading to object storage, and recovery of log position,
  manifests, and segment tails across a restart
- Durability tracking that marks a segment durable once it is in the bucket,
  so `acks=all` waits for storage rather than for a local flush
- Retention by age and by size
- Partition leases and ownership records, so a failover costs a degraded read
  path rather than a split brain
- A Prometheus endpoint with request, durability, throughput, and
  `kimistore_build_info` metrics
- `kimistore-credential` for SCRAM credentials and ACL rules
- A static site published to GitHub Pages
- Multi-platform container images for `linux/arm64` and `linux/amd64`, built
  with ko and published to `ghcr.io/kimistore/agent` and
  `ghcr.io/kimistore/agent-credential`. Both are signed keyless with cosign, so
  verification needs no key and a stolen registry credential cannot mint a
  certificate claiming to be this repository's build workflow. Every `vX.Y.Z`
  tag publishes a signed release image; `:main` tracks the branch
- `/ready` and `/live` on the metrics port, so a Kubernetes probe can tell a
  broker that should be sent clients apart from one that should merely be
  restarted. `/ready` reports 503 with a reason when partition claims cannot be
  renewed or the cluster view is stale

### Changed

- Partitions can be shared across agents by deterministic hash rather than won
  by whichever agent started first. Opt in with `KIMISTORE_ASSIGNMENT=rendezvous`.
  Off by default, because it changes which agent owns what
- A partition owner that has lost its claim to an agent using the same id is
  refused rather than renewing, so two agents sharing an id cannot hand a
  partition back and forth
- Partition manifests are only written after the claim is re-checked against the
  object store, so a writer that has lost its claim cannot overwrite its
  successor's record
- A `SIGTERM` seals, uploads, and records each partition before releasing it, so
  an orderly shutdown is a handover rather than a recovery

### Fixed

- The pending-upload counter went negative when a segment was re-uploaded by the
  reconciler, which made every shutdown wait out its full 30 second budget and
  report a negative number of segments still uploading

### Known limitations

These are deliberate for 1.0. Each one is a gap a user can hit.

- No replication. One node, one writer per partition
- ISR is always 1 and the high watermark is always the log end offset
- `Describe` is not enforced on consumer groups. Topic rules do not apply to a
  group coordinate
- The `TopicAuthorizedOperations` bitmask from Metadata v8 and DescribeConfigs
  is not emitted, so Metadata stops at v7
- The `Alter` ACL operation is accepted and stored but not enforced, because
  the agent implements neither AlterConfigs nor DeleteRecords
- The flexible, tagged-field encoding is not implemented. Produce stops at v3
  and every other ceiling sits at or below it
- PLAIN sends the password in the clear without TLS. SCRAM does not

## Earlier work

This section records the commits before the first tag, so the history is not
lost when the changelog starts. Read `git log` for the detail.

- Durable-timeout fix: a stale durability frontier left partitions waiting for
  an acknowledgement that had already been lost. Coverage moved from 86% to
  98% with no timeouts
- Race fixes in checkpoint rehydration and the server wait group
- Topic authorization on the Metadata path, and the read-error mapping that
  separates a reclaimed offset from a failed object store
