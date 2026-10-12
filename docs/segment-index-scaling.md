# Scaling the segment index

## The problem

The checkpoint carries one index entry per segment ever written:

```
segments in inventory:  36,586
checkpoint size:        ~2.5 MB (at 23k; ~4 MB now)
per entry:              ~106 bytes
checkpoint interval:    ~30s
```

Its cost is not its size, it is the product:

    writes = O(total segments x checkpoints)

At the measured ingest rate of 3,204 segments/hour that is ~11 GB/day on day
one and ~81 GB/day by day seven, on a node whose actual work is uploading
segments. At a million segments the same code writes ~100 MB per checkpoint.

## Why the shape is wrong

One object is doing two jobs with opposite write profiles.

| | size | write rate |
|---|---|---|
| Position: log end, log start, epoch, committed offsets | O(1) | every checkpoint |
| Segment index: offset -> key | O(history) | once per segment |

The index changes at the rate data arrives, not at the rate position advances.
Rewriting it every 30s is the defect.

## Measured read distribution

Sampled from the live consumer over four intervals:

```
leo=726810 committed=726809 lag=1  inventoryPos=36525/36526 (100.0%)
leo=726880 committed=726879 lag=1  inventoryPos=36555/36556 (100.0%)
leo=726980 committed=726979 lag=1  inventoryPos=36585/36586 (100.0%)
```

The consumer sits **one offset behind the log end**, always on the newest
segment. Reads are hot: they land in the newest segment or the one before it.

This sizes the open window and settles the cold-read question, but it does not
excuse the design. A consumer catching up after downtime, or any historical
read, is cold by definition and must still resolve. The index has to be correct
for cold reads; it merely does not need optimising for them at this workload.

## Design

Three tiers, split by write frequency.

### Tier 0 — the checkpoint

Written every checkpoint, as today, but now bounded:

    position   log end, log start, epoch, committed offsets
    sealed     directory of immutable buckets: [{first, last, key}]
    recent     the open window: newest N segments, full entries

`sealed` is O(history / N). `recent` is O(N). Neither grows with the write rate.

### Tier 1 — sealed buckets

Immutable objects at `_idx/<topic>/<partition>/<first>-<last>.json`, holding a
run of segments. Written **once**, when the open window fills. Never rewritten,
because the run they cover is finished by construction.

### Tier 2 — nothing

Bucket names encode their range and their order, so a bucket is found without
consulting anything.

## The property that matters

Total bytes written becomes:

    O(total segments)   amortised

instead of `O(total segments x checkpoints)`.

The open window is flushed to an immutable bucket only when it fills. That is
the entire scaling argument, and it depends on sealed buckets never being
rewritten — which is also why an append must never mutate a sealed bucket.

## Retention

Retention gets simpler, not harder.

Deleting reclaimed history becomes **deleting whole buckets**, one object per
bucket, and advancing `log_start` to the first surviving bucket's first offset.
No scan, and no rewrite of a large index.

The claim guard is unaffected: `FirstAllowedOffset` is already computed from
committed offsets across groups, not from the index.

A bucket may also be deleted in part, when reclamation starts inside it. The
bucket is rewritten in that case, which is the one place a sealed bucket is not
immutable, and it happens once per partially-reclaimed bucket rather than once
per segment.

## Rebuild

Today a lost index means listing every segment, which is the operation that
does not scale. Under this scheme the index is recovered by listing
`_idx/<topic>/<partition>/`, which is proportional to bucket count
(history / N), not segment count. A single suspect bucket is recovered from its
own prefix alone.

So the index stays repairable without reintroducing the operation that broke
this broker.

## Entry encoding

The object key is fully derivable from the partition prefix plus two integers:

    <topic>/<partition>/<020d base>-e<epoch>.log

so an entry is `SegmentRef{Base, Epoch int64}` — 16 bytes, against ~106 today
for the same information. With bucketing this no longer affects the hot path,
because the checkpoint no longer holds the whole index; it matters for bucket
storage and for the directory, both of which are written rarely.

## Open decisions

**Bucket size N.** Measured reads are hot, so N is chosen by write cost rather
than read latency. N=256 makes the window ~4 KB and the directory
(history/256). At 3,204 segments/hour a year of history is ~29M segments, a
116k-entry directory, ~1.9 MB in every checkpoint — too much. N=4096 gives a
7k-entry directory, ~115 KB, which at a year is still written every 30s.

So N alone does not fix the long run: **the directory eventually needs its own
tier.** The naming scheme should permit directories of directories from the
start, so this is a naming decision taken now rather than a rewrite later.

**Cold reads.** Every cold read costs one bucket GET plus a binary search. At
this workload that is rare, and an LRU is not justified by the measurement. The
cost is named here so it is a decision rather than a surprise.

## What this does not fix

- The inventory still describes history that retention has not reclaimed. That
  is inherent to not running retention, not to the index shape.
- `log_start` advancing does not delete buckets; retention does, and only
  retention.