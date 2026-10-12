# Changelog

All notable changes to Kimistore are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[semantic versioning](https://semver.org/spec/v2.0.0.html).

A version is `MAJOR.MINOR.PATCH`. The project is pre-1.0, so the minor number
carries the breaking changes and the patch number carries the fixes.

## Unreleased

Nothing yet.

## 1.0.7

Retention could not be enabled safely. Switching it on would have deleted about
11,000 objects on the first sweep and broken the one consumer on the partition.

### Fixed

- Retention no longer treats the consumer bound as permission to delete. Setting
  any retention value at all made every segment a consumer had passed
  reclaimable, so a log inside its budget lost everything. The age and byte
  policies now decide what goes, and the consumer bound only raises the
  resulting boundary. A healthy log inside budget deletes nothing, which is the
  normal state and what an operator switching retention on expects
- Retention no longer deletes the segment a consumer resumes from. The window
  trim dropped every entry whose base was below the boundary, on the reasoning
  that such an entry was entirely below it. A segment covers a range, so the one
  holding the committed offset almost always has a base just below it. The live
  shape was bases `777763`, `777764`, `[777765]`, `777766`: reclaiming at
  777765 removed 777764, and the consumer's next read resolved to no segment at
  all. The rule the straddling-bucket path already used is now used for the
  window too -- reclaimable only once its successor starts at or below the
  boundary
- Retention no longer indexes past the end of the index when the newest entry is
  itself over budget. That panicked; the existing tests had no sealed buckets
  and never reached it

### Changed

- `SegmentRef` carries segment size and modification time, recorded by the
  uploader from the PUT it had already made. `BucketRef` carries the byte total
  and newest time of what it covers, so a bucket is judged without being read
  back. Both policies previously needed a listing to evaluate, which is the one
  operation on this bucket that does not scale: a page of keys takes about 17
  seconds and a full pass does not finish, so a sweep that listed reclaimed
  nothing on a partition of any size. The listing-based size and time sweep is
  removed rather than left alongside, since two implementations of one policy
  is how they drift apart
- Reclaiming the unindexed legacy path applies the age policy and a cumulative
  byte budget as well, rather than the consumer bound alone

### Known limitations

- The age policy assumes segments were written in offset order, so age rises
  with base offset. That holds for anything this agent wrote. A segment
  re-uploaded later with a newer timestamp stops the scan early and holds back
  everything after it, which keeps data rather than deleting it
- A partition whose index is empty still falls back to a listing. That is the
  legacy case and does not grow, but on this bucket a listing cannot complete,
  so such a partition is reclaimed by neither path until its index is rebuilt

## 1.0.6

Retention reclaimed nothing. It could not: its sweep began by listing every
object in the partition, and on knode3 that listing does not finish. The sweep
logged the failure and moved on, once per interval, forever.

### Changed

- Retention reclaims through the index. `DropSegmentsBefore` had no caller, so
  sealed buckets would have accumulated regardless. A bucket wholly below the
  retention boundary is now one `DELETE` for the bucket plus one per segment it
  covers, and the index shrinks by that whole bucket
- A bucket the boundary passes through is rewritten without the reclaimed
  prefix. This is the one place a sealed bucket is not immutable, and it cannot
  be left alone: the entries it drops name segments that are about to be
  deleted, so a read of a reclaimed offset would resolve to an object that is
  gone instead of falling back. It happens at most once per bucket, as retention
  advances across it
- A segment counts as reclaimable only when its successor starts at or below the
  boundary, which is what proves every offset inside it has been consumed. The
  newest segment has no successor and is kept — the same rule the listing-based
  sweep applied, so retention does not become more aggressive
- The size and time policies still list. They select on segment size and
  last-modified time, which the index does not carry. They are kept separate from
  the index path deliberately: a partition whose listing cannot complete still
  loses the data no consumer needs, instead of losing the size and time
  behaviour as well

### Fixed

- Retention no longer reclaims nothing when the index is empty but segments are
  not. It fell back to a listing, bounded by how much unindexed data exists.
  Without that fallback this release would have leaked storage silently and
  forever, which is what the first draft of it did and what the existing
  retention tests caught
- A loaded bucket is no longer merged into the open window. Merging doubled the
  window on every cold read, so the checkpoint grew again, and retention deleted
  the same segment once through the bucket and again through the window. Loaded
  references are now searched directly; the window stays the newest segments

### Known limitations

- The sealed-bucket path has not yet run in production. On knode3 the window is
  at roughly 250 of 4096, so no bucket has been sealed, reclaimed, or rewritten.
  It is covered by unit tests only, including the straddling rewrite
- The size and time policies remain listing-bound. Carrying size and
  last-modified in `SegmentRef` would close that at the cost of a larger index
  entry

## 1.0.5

Every graceful shutdown was destroying the segment index.

### Fixed

- A shutdown drain no longer erases the durable position and index. The journal
  from knode3:

      Draining 1 owned partition(s) before shutdown, budget 25s
      Ownership: released mimir-distributor/0 at epoch 21405
      Drain: drained 1 of 1 partition(s); 0 kept their claim
      Saved metadata checkpoint to _agents/knode3/checkpoint.json (259 bytes)

  `DrainOwned` ran the same routine a peer takeover runs, and a handover drops the
  partition from the cache. On shutdown that is pure loss: the claim is released
  so a peer may take over, but nobody does, and this same agent is the next
  reader. The segments were still in object storage -- only the index describing
  them was gone, and rebuilding it needs the very listing the index exists to
  avoid. The next start found a 124-byte checkpoint and rebuilt the index from
  scratch at about 640 bytes a minute.
- `DrainPartition` takes a handoff flag. A real handover still erases the position
  and index, because the new owner recovers them from the manifest. A shutdown
  drain releases the claim and keeps them, dropping only the in-memory serving
  state that the next append rebuilds.
- An unowned partition is no longer removed from the metadata cache at startup.
  This was not the cause of the above, but removing durable state on an unowned
  check is unsafe regardless, and both paths that cared already guard
  themselves: `HighWaterMark` answers 0 for an unowned partition and Metadata emits
  `LEADER_NOT_AVAILABLE` when `routing.Owner` finds no claim.

The test reproduces the recorded failure: 745 bytes before the drain, 13 after.

## 1.0.4

1.0.3 added the window-and-bucket index but left recovery repopulating the flat
one, so the checkpoint still carried every segment ever written and still grew
with history. Measured on knode3 after deploying 1.0.3: 4.0 MB and 37,983
segments, with the new shape correctly present alongside the old one.

### Fixed

- The flat segment list is gone from the checkpoint and from the per-partition
  manifest. Recovery and retention no longer rebuild it by listing, which is
  what kept putting the whole log back on disk every checkpoint. The index is
  the open window and the sealed-bucket directory, maintained by the uploader
- A test now samples the checkpoint across a full window cycle and asserts a
  peak bounded by RecentWindow rather than by history. The previous suite
  passed while the checkpoint was still 4 MB

Measured peak across 32,760 segments: 24,937 bytes, against roughly 3.4 MB for
a flat index at the same point.

## 1.0.3

A consumer that could not resume, and a checkpoint that grew without bound.
Both were found on knode3 while restoring a partition to health after the
object-store listing a read used to rely on could no longer complete.

### Fixed

- An `OffsetFetch` naming no topics now answers with the group's committed
  offsets. An empty topic list means "every topic this group has a position
  for", which is how Mimir's ingester asks. The empty answer made it fall back
  to `auto.offset.reset` and fetch from zero, ignoring a committed offset that
  was durably stored the whole time. The failure is silent: the consumer
  connects and produces normally, it just never catches up
- The segment index keeps one entry per base offset, newest epoch winning. A
  re-claimed partition writes the same offset again under a newer epoch, and
  both objects exist in the bucket; keeping both made the lookup choose between
  them by position and return the superseded one, serving a consumer data the
  current owner had already replaced

### Changed

- The checkpoint no longer carries the whole segment index. It holds the
  position, an open window of the newest 4096 segments, and a directory of
  sealed buckets; a full window is sealed into an immutable bucket written
  once. Total bytes written becomes O(total segments) rather than
  O(total segments x checkpoints). Measured on knode3 at 3204 segments/hour the
  flat index had reached 4 MB per checkpoint and was growing towards 80 GB/day
- Retention deletes whole buckets and advances the log start, instead of
  rebuilding a full segment list to decide what to remove
- An index entry is a base offset and an epoch rather than a stored object key,
  16 bytes against ~106

### Known limitations

- The index describes only what this release recorded. Segments uploaded before
  it were never indexed and cannot be read back, because doing so needs the
  listing this index exists to avoid. A consumer resumes from the oldest segment
  recorded since the upgrade
- The directory of sealed buckets still grows at history/4096. That is slow
  enough to be a later problem, but the naming scheme should permit a second
  level when it arrives

## 1.0.2

The object-store listing that a read used to locate a segment is gone from the
read path, and so is the listing that used to rebuild it.

Listing a partition grows with that partition's entire history, and it stops
completing well before a partition gets large: against RustFS a single 1000-key
page took over 60 seconds and a full pass did not finish in 200. Since an
object-store call is bounded by the 30 second operation timeout, every read for
an offset below the local WAL failed. A consumer could not catch up, and a read
path that fails is an outage rather than a slow read.

This was the same slow listing behind the 1.0.1 upload deadlock. That release
stopped it starving the upload workers; this removes it.

### Fixed

- A read resolves the segment covering an offset from the manifest's segment
  inventory, by binary search, instead of listing the partition prefix. The
  listing remains only as a fallback for a partition with no inventory yet
- Each uploaded segment is recorded in the inventory and marks its manifest
  dirty, so the index survives a restart and recovery never lists. Before this,
  recovery rebuilt the inventory by listing, which timed out, so a restarted
  agent came up with an empty inventory and every cold read fell back to the
  listing that had just failed
- Recording a segment already present replaces it rather than appending, so a
  reconciliation re-upload cannot grow the manifest without bound

### Known limitations

- The inventory only covers segments uploaded after upgrading to this release.
  History written by an earlier version was never recorded and cannot be read
  back, because doing so needs the listing this release removes. Backfill it by
  replaying the partition, or accept that consumers resume from the oldest
  segment recorded since the upgrade

## 1.0.1

A read-path deadlock that could stop the log uploading without stopping the
broker from accepting connections.

### Fixed

- The segment cache no longer holds its lock across an object-store LIST.
  `handleUpload` needs that same lock to record a finished upload, so a slow
  LIST under concurrent reads could starve every upload worker. Nothing would
  then drain the upload queue, sealed segments would pile up on local disk, and
  every `acks=all` producer would time out while the broker still answered
  metadata and health probes. Observed on knode3 with all eight workers parked
  for 17.7 hours and 15,133 segments stranded; see `fix: stop holding the
  segment cache lock across a LIST`

### Changed

- Concurrent cache misses on one partition issue a single LIST rather than one
  per reader, via a per-key flight rather than a global lock
- A failed LIST is memoised for five seconds, so an object store that is down
  is not asked once per reader
- Retention invalidation bumps a per-key generation, so a LIST already in
  flight when segments were deleted is discarded instead of republishing them

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
