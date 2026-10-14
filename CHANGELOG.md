# Changelog

All notable changes to Kimistore are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[semantic versioning](https://semver.org/spec/v2.0.0.html).

A version is `MAJOR.MINOR.PATCH`. The project is pre-1.0, so the minor number
carries the breaking changes and the patch number carries the fixes.

## Unreleased

Nothing yet.

## 1.0.16

Compaction stopped entirely once the backlog was cleared, and said nothing while
it did.

### Fixed

- A bucket that is already fully merged no longer stops the queue. Once the
  backlog on knode3 was compacted, the oldest bucket held nothing but merged
  products, every one at or above half the merged-size cap, so the selection
  skipped every entry and the pass had nothing to do. Candidate selection
  accepted that bucket anyway, because it only checked whether the entries
  carried sizes and never whether a pass would actually pick any, so it won the
  queue and returned zero

  The result was silence: no log line, no error, and the object count rising by
  48,000 over eight hours while a search for "Compactor" showed the last merge
  at 00:05. Candidate selection and the pass now use one `selectSources`, so a
  bucket is only taken when a pass over it would have something to do

## 1.0.15

Fixes a data-loss defect in the compactor. It destroyed seven offsets on knode3
before this release.

### Fixed

- A merge no longer derives the rewritten bucket by position. Selection steps
  over entries that are already merged products, so the sources it chooses are a
  subset of the bucket rather than a prefix of it. The rewritten bucket was
  built as `refs[len(sources):]`, which assumes the opposite, and so:

  - listed entries that had been merged -- whose objects the pass then deleted,
    leaving the index naming segments that no longer exist, and
  - dropped entries that had been left alone, taking their index entries with
    them

  The surviving sources are now tracked by position and the rest selected by
  membership

- A merge no longer narrows its bucket's range. Taking the range from the
  rewritten contents collapses it onto the merged segment's base offset whenever a
  pass merges the whole bucket, which puts every offset above that base outside
  every bucket and makes those reads unresolvable. A merge consolidates the
  entries in a range; it does not change the range, which is the original
  bucket's first and last base offsets, as `SealWindow` derives it

- The rewritten bucket's contents are sorted explicitly. A stepped-over entry at
  the head leaves the merged segment's base above the bucket's first, and
  `BucketFor` binary-searches the list, so an unsorted one is silently missed

### Known limitations

- The seven offsets lost on knode3 are not recoverable; no object holds them.
  They were each the tail of a compacted bucket, and reads for them fail

## 1.0.14

An index entry naming a segment that no longer exists no longer stops compaction.

### Fixed

- A merge skips an index entry whose object is absent instead of aborting. The
  absence cannot be cured by retrying, so retrying is what turned one bad entry
  into a permanently stalled partition: on knode3 every pass failed on the same
  segment, once a minute, for hours, and a log search for "Compactor" showed
  activity throughout. The entry is dropped from the merge and the log start
  moves past it, because a gap no object can fill is better reported by
  ListOffsets than by a read that never resolves

  This state was reached by the entry-dropping bug fixed in 1.0.13, which had
  already damaged one bucket before it shipped. A repair run cleared it: 1 entry
  of 61,000 named a missing object, now zero

### Known limitations

- An object that is absent cannot be recovered. The repair removes it from the
  index, which turns a read failure into a visible gap rather than a silent
  one, but the data at that offset is gone

## 1.0.13

Compaction was losing the object-count race badly, and shortening the interval
would not have fixed it.

### Fixed

- A merge pass takes segments until the size cap rather than stopping at the
  minimum. The selection condition combined the two --
  `len(sources) >= MinSegments && bytesIn >= MaxMergedBytes` -- so once a bucket's
  first entry was itself a merged segment, the byte test was already satisfied on
  the first iteration and every pass merged exactly eight segments. Against
  3,200 segments an hour that is losing by two orders of magnitude

- A partially compacted bucket no longer stalls after its first pass. A merged
  segment lands just under the cap, because the loop stops before an entry would
  overshoot, so an exact cap comparison let that segment through as the first
  entry of the next pass -- where it consumed the entire budget and left the
  pass with nothing to merge. Segments at least half the cap are now treated as
  previous passes' products and stepped over. Measured on the test fixture: 8
  segments per pass before, 57 after

- Entries a pass did not merge stay in the rewritten bucket. They were being
  dropped from the index: still present in object storage, unreachable, and a
  read for their offsets would fail

### Changed

- The compaction interval defaults to 60s rather than 15 minutes, and is
  configurable with `KIMISTORE_COMPACTION_INTERVAL_MS`. Set
  `KIMISTORE_COMPACTION_DISABLED=true` to turn compaction off. The pass is
  cheap -- it merges up to `MaxMergedBytes`, seconds of I/O -- so the interval
  only decides whether the budget is spent faster than the partition fills

### Known limitations

- Compaction consolidates sealed buckets only. The open window and the newest
  sealed bucket are never merged, so the steady-state object count is bounded by
  the window rather than falling to the number of merged segments alone
- A merge interrupted after writing its bucket but before committing the index
  leaves an unreferenced merged segment that no marker covers. A later merge
  overwrites it, since the merged key derives from the same offsets

## 1.0.12

Fixes a log start that could rewind past the data still present.

### Fixed

- The log start no longer falls below the oldest offset the segment index
  describes. Two paths could rewind it. Restoring a per-partition manifest ran
  after the checkpoint load and took the manifest's `log_start_offset` verbatim,
  so a manifest left behind by a sweep rewound the start past data the index
  still described. `SetPartitionState` now clamps it to the oldest indexed entry

  The consequence was a consumer that resets to the start of the log being sent
  to an offset that resolves to nothing, looping on `OffsetOutOfRange` with
  nothing in the broker's logs to say why. Observed on knode3 after its legacy
  buckets were dropped by hand: the checkpoint said 781330, the manifest still
  said 0, and the manifest won

- Lowering the start is never the safe direction. The index cannot name a
  segment below its oldest entry, so a start above it is already conservative,
  while one below it is a claim about data that is gone. Forward movement is
  unaffected, so retention still advances the start when it reclaims

## 1.0.11

The compactor had never merged anything, and the reason was a bug rather than
the data it was waiting for.

### Fixed

- A bucket that could not be merged no longer blocks every bucket behind it. The
  compactor took only the oldest candidate and returned when it could not merge
  it, so three buckets recorded before the index carried sizes sat at the front
  of the queue and starved the fourteen compactable buckets behind them. Every
  15-minute tick logged the same skip and the object count did not move:

      Compactor: mimir-distributor/0: bucket ...00000000000000781330-00000000000000793734.idx
      holds entries recorded before sizes were indexed; skipping

  That ran 23 hours on knode3 with zero merges. Candidate selection now scans
  forward to the first bucket that can actually be merged. The list is still
  oldest-first and the oldest mergeable range is still compacted first, so
  precedence is unchanged
- Whether an entry has a recorded size is now decided while choosing a
  candidate, not after. Testing it once a bucket had been chosen turned a skip
  into an abort, and the abort ended the pass that had already found a workable
  bucket further along

### Known limitations

- The three legacy buckets remain unmergeable until the window has rolled past
  them, since a merged size cannot be capped for entries of unknown size. They
  are skipped, not blocking, from this release
- Recovery cannot reclaim a merged segment written but not yet committed: no
  marker covers it, and its key derives from the same offsets, so a later merge
  overwrites it

## 1.0.10

Corrects the segment size chosen in 1.0.8, which was raised when it should have
been lowered.

### Fixed

- `MaxSegmentSize` is 16MB, not the 256MB set in 1.0.8. The reasoning given then
  was that a read "seeks with the sidecar and stops at the fetch budget, so a
  larger segment does not make a read proportional to the segment -- only to the
  position within it". That first claim is true; the conclusion did not follow,
  because it left out the sidecar itself. The index is written every 4KB at 8
  bytes per entry, so it grows linearly with the segment -- 32KB at 16MB, 128KB at
  64MB, 512KB at 256MB -- and every read fetches it in full before returning the
  first record. The fetch budget bounds only the record data, so raising the
  threshold fourfold made the fixed cost of every read grow fourfold as well.
  A test now holds the sidecar under a 64KB ceiling and fails at 256MB

### Known limitations

- A merge reads every source and writes the result, so the merged size is also
  the per-pass cost of compaction on a single-threaded loop sharing the box with
  the uploader. 16MB keeps a pass to seconds rather than minutes
- Recovery cannot reclaim a merged segment written but not yet committed: no
  marker covers it, and its key is derived from the same offsets, so a later
  merge overwrites it

## 1.0.9

Recovers a merge that a crash interrupted, and fixes two bugs in the merge path
that only a full-bucket compaction would have reached.

### Fixed

- A merge that committed its index update and then died before deleting its
  sources left them orphaned: unreferenced, so nothing could read them, and
  unreachable by any listing, so nothing could reclaim them. The marker written
  just after the commit is now read at startup and its sources deleted. Markers
  are named after the bucket that replaced them, so recovery finds them through
  the checkpoint's own bucket directory -- one probe per sealed bucket, against a
  listing that cannot complete on this bucket at all. A marker whose bucket the
  index does not name belongs to a merge that never committed, and its sources
  are still live, so it is left alone
- A merged bucket's `Last` was computed as the first base offset plus the entry
  count. Offsets do not advance one per entry -- a segment's base is the offset
  its first record carries, and entries skip -- so on any real partition the
  range was overstated, which would make `BucketFor` resolve an offset to the
  wrong bucket. It now takes the last source entry's base, as `SealWindow` does
- A merge that consumed an entire bucket produced a bucket with the same key as
  the one it replaced, because the key derives from the first and last offsets.
  The old bucket was then deleted, removing the very bucket the index had just
  been pointed at, and every read in that range would have failed to resolve.
  The delete is now skipped when the key is unchanged

### Known limitations

- Recovery is limited to the partition's own markers. A merge interrupted after
  writing its bucket but before committing the index leaves an unreferenced
  merged segment that no marker covers, and nothing reclaims it. It costs storage
  and nothing else, and a later merge overwrites it, because the merged key is
  derived from the same offsets
- Segments recorded before the index carried sizes are refused by the
  compactor, because the cap on merged size cannot be applied to entries of
  unknown size. They are compacted once the window has rolled past them

## 1.0.8

The compactor is on by default and the segment size is settled.

### Changed

- Background compaction of sealed buckets is now on unless an operator sets
  `CompactorConfig.Disabled`. It was opt-in, which meant the object count a
  partition accumulated was governed by the flush interval and nothing brought
  it down. The flag is inverted to `Disabled` because the zero value of a bool
  cannot mean "on by default", and an opt-in flag is exactly how the previous
  version ended up off
- `MaxSegmentSize` is raised from 64MB to 256MB. The old value was never the
  binding constraint: a partition carrying `acks=all` traffic is sealed by the
  flush interval rather than by size, and segments measured at 45KB against a
  64MB threshold. What the threshold actually decided was the size of a merged
  segment, which is what the object count converges to. A read seeks with the
  sidecar and stops at the fetch budget, so a larger segment does not make a
  read proportional to the segment -- only to the position within it

### Known limitations

- A merge that committed its index update and then died before deleting its
  sources leaves those sources orphaned. They are unreferenced, so they cost
  storage and nothing else, and no listing could find them again. The marker
  written just after the commit exists to recover this and nothing reads it yet.
  It is left as an explicit gap rather than a stub reporting success
- Segments recorded before the index carried sizes are refused by the
  compactor, because the cap on merged size cannot be applied to entries of
  unknown size. They are compacted once the window has rolled past them

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

- A size the index does not know is counted against the budget rather than as
  free. Entries recorded before `SegmentRef` carried sizes have none, and
  summing them as zero accounted 4.2 MB against roughly 500 MB of real segments
  on knode3, so a budget set from actual disk usage would never have fired and
  retention would have looked configured while doing nothing. Treating an
  unknown size as over budget errs towards reclaiming, and the consumer bound
  still applies on top of it

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
