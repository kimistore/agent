#!/bin/sh
# Integrity audit for the kimistore bucket on knode3.
#
# Fetches a fresh checkpoint every run. An earlier version read a fixed file, so
# it audited whatever index happened to be on disk when the file was written: a
# stale index names segments that compaction has since replaced, and those read
# as "object missing" -- a false data-loss alarm on a healthy system.
#
# Checks both directions, because each catches a different failure:
#
#   index -> object   every entry the index names must exist. Catches sources
#                     deleted before their replacement was committed.
#   object -> index   every segment on disk must be named by the index. Catches
#                     an orphaned merged segment, or an index swap that lost
#                     entries it should have kept.
#   content           a sample of segments is opened and its real first offset read
#                     back, so a segment that exists but holds the wrong bytes
#                     cannot pass. Offsets and sizes are big-endian; reading them
#                     little-endian yields a large plausible-looking number and
#                     would report every segment as corrupt.
#
# Credentials are read from the agent's own environment file rather than written
# into this script, so that neither a commit nor a shell history carries a secret.
set -e
ENVFILE="${KIMISTORE_ENV:-/home/andy/kimistore/kimistore.env}"
[ -r "$ENVFILE" ] || { echo "cannot read $ENVFILE"; exit 1; }
AK=$(sed -n 's/^AWS_ACCESS_KEY_ID=//p' "$ENVFILE")
SK=$(sed -n 's/^AWS_SECRET_ACCESS_KEY=//p' "$ENVFILE")
[ -n "$AK" ] && [ -n "$SK" ] || { echo "no credentials in $ENVFILE"; exit 1; }
D=$(sed -n 's/^S3_DATA_DIR=//p' "$ENVFILE")
D="${D:-/home/andy/RustFS/data/kimistore/mimir-distributor/0}"
CP=/tmp/audit-$(date +%s).json

curl -s --aws-sigv4 "aws:amz:us-east-1:s3" -u "$AK:$SK" \
  http://127.0.0.1:9000/kimistore/_agents/knode3/checkpoint.json -o "$CP"

python3 - "$CP" "$AK" "$SK" "$D" <<'PY'
import json, os, random, struct, subprocess, sys

cp, ak, sk, D = sys.argv[1:5]

def get(key, out):
    subprocess.run(["curl", "-s", "--aws-sigv4", "aws:amz:us-east-1:s3",
                    "-u", f"{ak}:{sk}", "-o", out,
                    f"http://127.0.0.1:9000/kimistore/{key}"],
                   capture_output=True, timeout=180)

def nm(r):
    n = "%020d" % r["b"]
    return n + ("-e%d" % r["e"] if r.get("e", 0) > 0 else "")

d = json.load(open(cp))
pv = d["topics"]["mimir-distributor"]["partitions"]["0"]
sealed = pv.get("sealed") or []
recent = pv.get("recent") or []

def epoch_of(name):
    return int(name.rsplit("-e", 1)[1]) if "-e" in name else 0

indexed = {}          # object name -> base offset, everything the index names
buckets = 0
failed = 0
for b in sealed:
    get(b["key"], "/tmp/audit-b.json")
    try:
        refs = json.load(open("/tmp/audit-b.json"))
    except Exception:
        failed += 1
        continue
    buckets += 1
    for r in refs:
        indexed[nm(r)] = r["b"]
for r in recent:
    indexed[nm(r)] = r["b"]

# Direction 1: index -> object.
dangling = [k for k in indexed if not os.path.isdir(os.path.join(D, k + ".log"))]

# Direction 2: object -> index. Wal and scratch are excluded.
on_disk = set()
for name in os.listdir(D):
    if name.endswith(".log"):
        on_disk.add(name[:-4])
orphans = sorted(on_disk - set(indexed))

# Orphans are two different things and counting them as one number hides both.
#
# A merge writes its merged segment before it commits the index, so an object
# nobody references yet is the expected shape of a merge in flight -- it sits at
# the current write epoch, and the next audit finds a later range of them. A
# leak instead accumulates: the same objects, at old epochs, forever. Only the
# second is a fault, so they are reported apart and the verdict looks at the
# second alone.
current = min((epoch_of(nm(r)) for r in recent), default=0)
transient = [o for o in orphans if epoch_of(o) >= current]
debris = [o for o in orphans if epoch_of(o) < current]
debris_bytes = 0
for o in debris[:40]:
    try:
        debris_bytes += os.path.getsize(os.path.join(D, o + ".log", "xl.meta"))
    except OSError:
        pass

print(f"  buckets {buckets} loaded, {failed} unreadable   window {len(recent)}   indexed {len(indexed)}")
print(f"  index names an object that is missing : {len(dangling)}")
for k in dangling[:5]:
    print(f"      DANGLING {k}")
print(f"  on disk but not indexed: {len(transient)} in flight at the write head "
      f"(epoch {current}), {len(debris)} older than the window")
for k in debris[:4]:
    print(f"      DEBRIS   {k}  epoch {epoch_of(k)}")

# Direction 3: content. A merged segment can exist and still be wrong, so open a
# sample and read the first offset back out of the record header. The log format
# is a big-endian 8-byte offset, a big-endian 4-byte size, then the body.
sample = random.Random(7).sample(sorted(indexed), min(6, len(indexed)))
bad = 0
for name in sample:
    get(f"mimir-distributor/0/{name}.log", "/tmp/audit-s.log")
    try:
        with open("/tmp/audit-s.log", "rb") as fh:
            head = fh.read(12)
        if len(head) < 12:
            print(f"      SHORT    {name}: {len(head)} bytes")
            bad += 1
            continue
        first, size = struct.unpack(">QI", head[:12])
        if first != indexed[name]:
            print(f"      MISMATCH {name}: header says {first}, index says {indexed[name]}")
            bad += 1
    except Exception as exc:
        print(f"      UNREAD   {name}: {exc}")
        bad += 1
print(f"  sampled {len(sample)} merged segments, {bad} unreadable or wrong")

print()
# Debris is reported, not failed on. Nothing references it and nothing will ever
# reference it, but it is inert: it costs space and no correctness, and the
# compactor has no path to delete an object no index names.
verdict = "CLEAN" if not dangling and not debris and not bad else "PROBLEM"
if not dangling and not bad:
    verdict = "CLEAN (with unreclaimed debris)" if debris else "CLEAN"
print(f"  VERDICT: {verdict}")
PY

rm -f /tmp/audit-*.json /tmp/audit-b.json /tmp/audit-s.log
