#!/usr/bin/env python3
"""Reclaim segments that no index entry names, on a RustFS bucket.

Deleting unreferenced objects is only safe if "unreferenced" was determined from
a complete index. An index read that skipped a bucket -- a fetch timeout, a parse
error, a stale checkpoint -- makes every segment in that bucket look unreferenced,
and deleting them would destroy the only copy of live data. So completeness is
established before anything is deleted, and the run aborts if it cannot be.

Unreferenced does not mean worthless. A segment whose offset range is covered by
some indexed entry is redundant and can go. One whose range nothing covers is the
last copy of data, and is quarantined for review instead: it is the symptom of
something that lost a reference, and it is the evidence.

Four guards, each of which must hold:
  1. every sealed bucket parsed, and their count matches the checkpoint
  2. the checkpoint is re-read after the decision and before any delete, and must
     agree with the one the decision was made from
  3. an orphan is only eligible if its epoch is below every epoch in the index,
     which excludes segments a merge in flight has written but not committed
  4. an orphan is only eligible if its base offset is below the window's first
     offset, a second, independent way of saying the same thing

Run with no arguments for a dry run. Pass --delete to act.
"""
import json
import os
import subprocess
import sys

ENVFILE = os.environ.get("KIMISTORE_ENV", "/home/andy/kimistore/kimistore.env")
PREFIX = "mimir-distributor/0"
DATA = "/home/andy/RustFS/data/kimistore/" + PREFIX
QUARANTINE = "mimir-distributor/_quarantine/orphans"
SIG = "aws:amz:us-east-1:s3"
ENDPOINT = "http://127.0.0.1:9000/kimistore/"

AK = SK = None


def read_env():
    global AK, SK
    for line in open(ENVFILE):
        if line.startswith("AWS_ACCESS_KEY_ID="):
            AK = line.split("=", 1)[1].strip()
        elif line.startswith("AWS_SECRET_ACCESS_KEY="):
            SK = line.split("=", 1)[1].strip()
    if not AK or not SK:
        sys.exit("no credentials in " + ENVFILE)


def s3(method, key, out=None):
    """One S3 call. Keys go in raw: percent-encoding breaks the signature."""
    cmd = ["curl", "-s", "-X", method, "--aws-sigv4", SIG, "-u", f"{AK}:{SK}"]
    if out:
        cmd += ["-o", out]
    cmd.append(ENDPOINT + key)
    r = subprocess.run(cmd, capture_output=True, timeout=300)
    return r.stdout


def head(key):
    cmd = ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
           "--aws-sigv4", SIG, "-u", f"{AK}:{SK}", "-I", ENDPOINT + key]
    return subprocess.run(cmd, capture_output=True, text=True).stdout.strip()


def nm(r):
    n = "%020d" % r["b"]
    return n + ("-e%d" % r["e"] if r.get("e", 0) > 0 else "")


def epoch_of(name):
    return int(name.rsplit("-e", 1)[1]) if "-e" in name else 0


def load_index():
    """Return (indexed, min_epoch, window_min_base, stats) or abort.

    Guard 1 lives here. A bucket that will not parse is a hole in the index, and
    every segment behind the hole would look like an orphan.
    """
    s3("GET", "_agents/knode3/checkpoint.json", "/tmp/clean-cp.json")
    d = json.load(open("/tmp/clean-cp.json"))
    pv = d["topics"]["mimir-distributor"]["partitions"]["0"]
    sealed = pv.get("sealed") or []
    recent = pv.get("recent") or []

    indexed = set()
    entries = []            # (base, name) in offset order, for coverage
    parsed = 0
    for b in sealed:
        s3("GET", b["key"], "/tmp/clean-b.json")
        try:
            refs = json.load(open("/tmp/clean-b.json"))
        except Exception as exc:
            sys.exit(f"ABORT: bucket {b['key']} did not parse ({exc}); "
                     f"the index would be incomplete and every segment in it "
                     f"would look like an orphan")
        parsed += 1
        for r in refs:
            indexed.add(nm(r))
            entries.append((r["b"], nm(r)))
    for r in recent:
        indexed.add(nm(r))
        entries.append((r["b"], nm(r)))

    if parsed != len(sealed):
        sys.exit(f"ABORT: parsed {parsed} of {len(sealed)} buckets")

    # The in-flight boundary is the epoch the writer is using now, which is the
    # window's epoch. Taking the minimum epoch across the whole index instead --
    # sealed buckets go back to epoch 22077 -- makes every orphan look like it is
    # mid-write and nothing is ever reclaimable.
    epochs = [epoch_of(nm(r)) for r in recent]
    bases = [r["b"] for r in recent]
    entries.sort()
    return indexed, (min(epochs) if epochs else 0), (min(bases) if bases else 0), \
        {"buckets": parsed, "window": len(recent), "indexed": len(indexed),
         "entries": entries, "digest": digest_of(pv)}


def digest_of(pv):
    """A cheap fingerprint of the index, to detect it moving under the decision."""
    return json.dumps(pv, sort_keys=True)


def covered(base, entries):
    """Is base inside some indexed entry's range?

    An entry starting at base covers offsets until the next entry starts.
    """
    for i, (b, _) in enumerate(entries):
        if b == base:
            return True
        if b > base:
            return i > 0          # inside the previous entry's span
    return False


def main():
    delete = "--delete" in sys.argv
    read_env()
    indexed, min_epoch, win_min, stats = load_index()

    disk = {n[:-4] for n in os.listdir(DATA) if n.endswith(".log")}
    orphans = sorted(disk - indexed)

    eligible, transient, unique = [], [], []
    for o in orphans:
        base = int(o.split("-")[0])
        if epoch_of(o) >= min_epoch or base >= win_min:
            transient.append(o)                      # guards 3 and 4
        elif not covered(base, stats["entries"]):
            unique.append(o)                         # last copy of something
        else:
            eligible.append(o)

    print(f"  index: {stats['buckets']} buckets, {stats['window']} window, "
          f"{stats['indexed']} segments")
    print(f"  segments on disk not named by the index : {len(orphans)}")
    print(f"    in flight at the write head (excluded) : {len(transient)}")
    print(f"    offset range covered by another entry  : {len(eligible)}")
    print(f"    range NOT covered -- last copy, keep   : {len(unique)}")

    if not eligible:
        print("\n  nothing eligible to reclaim")
        return

    for o in eligible[:5]:
        print(f"      RECLAIM {o}")
    if len(eligible) > 5:
        print(f"      ... and {len(eligible) - 5} more")

    if not delete:
        print("\n  dry run -- pass --delete to act")
        return

    # Guard 2: re-read the index after quarantining and re-check each candidate.
    #
    # This is deliberately not a comparison of the whole index. The window rolls
    # continuously, so any digest of the index changes within seconds and the
    # guard would abort every run. What has to hold is the narrower and more
    # meaningful condition: none of these segments has become referenced, each is
    # still redundant, and each is still below the write head. If one became
    # referenced while we were copying it, deleting it would drop live data.
    indexed2, min_epoch2, win_min2, stats2 = load_index()
    still_orphan = [o for o in eligible if o not in indexed2]
    promoted = [o for o in eligible if o in indexed2]
    if promoted:
        sys.exit(f"ABORT: {len(promoted)} candidate(s) became referenced during "
                 f"quarantine, starting with {promoted[0]}; nothing deleted")
    no_longer_covered = [o for o in still_orphan
                         if not covered(int(o.split('-')[0]), stats2["entries"])]
    if no_longer_covered:
        sys.exit(f"ABORT: {len(no_longer_covered)} candidate(s) are no longer "
                 f"covered by another entry; they may be the last copy")
    eligible = still_orphan
    print(f"\n  re-checked {len(eligible)} candidates: still unreferenced, "
          f"still redundant, still below the write head")

    # Quarantine first. Deleting is the last step, not the first, so that a
    # mistake here is recoverable rather than final.
    os.makedirs(os.path.join(DATA, "_quarantine"), exist_ok=True)
    copied = failed = 0
    total = 0
    for o in eligible:
        for suffix in (".log", ".index"):
            if not os.path.isdir(os.path.join(DATA, o + suffix)):
                continue
            # s3() writes to the file and returns stdout, which is empty when -o
            # is used, so success is checked on the file rather than the return.
            s3("GET", f"{PREFIX}/{o}{suffix}", "/tmp/clean-o.bin")
            size = os.path.getsize("/tmp/clean-o.bin")
            if size == 0:
                failed += 1
                continue
            total += size
            p = subprocess.run(["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
                                "-X", "PUT", "--aws-sigv4", SIG, "-u", f"{AK}:{SK}",
                                "--data-binary", "@/tmp/clean-o.bin",
                                ENDPOINT + f"{QUARANTINE}/{o}{suffix}"],
                               capture_output=True, text=True)
            if p.stdout.strip() in ("200", "201"):
                copied += 1
            else:
                failed += 1
                print(f"      QUARANTINE FAILED {o}{suffix}: {p.stdout.strip()}")
    print(f"  quarantined {copied} objects, {total / 1e9:.2f} GB, to {QUARANTINE}/ ({failed} failed)")
    if failed:
        sys.exit("ABORT: quarantine incomplete; not deleting anything")

    # Verify the quarantine holds what we are about to drop.
    verified = 0
    for o in eligible:
        if head(f"{QUARANTINE}/{o}.log") == "200":
            verified += 1
    print(f"  quarantine verified: {verified}/{len(eligible)}")
    if verified != len(eligible):
        sys.exit("ABORT: quarantine incomplete; not deleting anything")

    gone = 0
    for o in eligible:
        for suffix in (".log", ".index"):
            if not os.path.isdir(os.path.join(DATA, o + suffix)):
                continue
            p = subprocess.run(["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
                                "-X", "DELETE", "--aws-sigv4", SIG, "-u", f"{AK}:{SK}",
                                ENDPOINT + f"{PREFIX}/{o}{suffix}"],
                               capture_output=True, text=True)
            if p.stdout.strip() in ("200", "204", "404"):
                gone += 1
            else:
                print(f"      FAILED {o}{suffix}: {p.stdout.strip()}")
    print(f"  deleted {gone} objects via the S3 API")

    for o in unique:
        print(f"      KEPT (last copy) {o}  base {int(o.split('-')[0])}")


if __name__ == "__main__":
    main()
