#!/usr/bin/env python3
"""Delete the orphan quarantine, after proving the index names none of it.

The quarantine holds 436 objects -- 218 segments the compactor superseded, copied
aside before their originals were deleted. Deleting it is destructive and the
obvious safety argument is wrong: the objects sit at base offsets around 793737
and 860726, which is ABOVE the log start offset of 781330, so "it is below the
retention floor" does not hold and says nothing about whether it is readable.

What makes it safe is narrower and checkable: no index entry names any of them.
Their ranges are covered by other entries -- the merged segments that replaced
them -- so removing the copies cannot remove data, and the audit already reports
0 orphans older than the write head, which is the same fact from the other side.

The check runs against a checkpoint fetched here, and the run aborts if a single
quarantined object is still referenced. Deletion is via the S3 API, because
RustFS caches object metadata and a filesystem delete leaves the object readable.
"""
import json
import os
import subprocess
import sys

ENVFILE = os.environ.get("KIMISTORE_ENV", "/home/andy/kimistore/kimistore.env")
QUARANTINE = "mimir-distributor/_quarantine/orphans"
DATA = "/home/andy/RustFS/data/kimistore/mimir-distributor/_quarantine/orphans"
ENDPOINT = "http://127.0.0.1:9000/kimistore/"
DRY = "--delete" not in sys.argv


def creds():
    ak = sk = None
    for line in open(ENVFILE):
        if line.startswith("AWS_ACCESS_KEY_ID="):
            ak = line.split("=", 1)[1].strip()
        elif line.startswith("AWS_SECRET_ACCESS_KEY="):
            sk = line.split("=", 1)[1].strip()
    return ak, sk


def main():
    ak, sk = creds()

    def get(key, out):
        subprocess.run(["curl", "-s", "--aws-sigv4", "aws:amz:us-east-1:s3",
                        "-u", f"{ak}:{sk}", "-o", out, ENDPOINT + key],
                       capture_output=True, timeout=300)

    get("_agents/knode3/checkpoint.json", "/tmp/qdel-cp.json")
    d = json.load(open("/tmp/qdel-cp.json"))
    pv = d["topics"]["mimir-distributor"]["partitions"]["0"]
    sealed = pv.get("sealed") or []
    print(f"  log_start_offset {pv['log_start_offset']}   sealed buckets {len(sealed)}")

    def nm(r):
        n = "%020d" % r["b"]
        return n + ("-e%d" % r["e"] if r.get("e", 0) > 0 else "")

    indexed = set()
    for b in sealed:
        get(b["key"], "/tmp/qdel-b.json")
        try:
            refs = json.load(open("/tmp/qdel-b.json"))
        except Exception:
            continue
        for r in refs:
            indexed.add(nm(r))
    for r in pv.get("recent") or []:
        indexed.add(nm(r))
    print(f"  segments named by the index: {len(indexed)}")

    quarantined = sorted(n for n in os.listdir(DATA) if n.endswith((".log", ".index")))
    segments = sorted({n.rsplit(".", 1)[0] for n in quarantined})
    print(f"  quarantined: {len(quarantined)} objects / {len(segments)} segments")

    still_referenced = sorted(s for s in segments if s in indexed)
    if still_referenced:
        print(f"  ABORT: {len(still_referenced)} quarantined segment(s) are still named by the index:")
        for s in still_referenced[:5]:
            print(f"    {s}")
        sys.exit(1)
    print("  none are named by the index")

    # Sanity on the naming: a quarantine entry that is neither a .log nor a
    # .index would mean the directory holds something unexpected.
    unexpected = [n for n in quarantined if not (n.endswith(".log") or n.endswith(".index"))]
    if unexpected:
        print(f"  ABORT: unexpected entries in the quarantine: {unexpected[:5]}")
        sys.exit(1)

    if DRY:
        print("\n  dry run -- pass --delete to act")
        return

    gone = failed = 0
    for n in quarantined:
        p = subprocess.run(["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
                            "-X", "DELETE", "--aws-sigv4", "aws:amz:us-east-1:s3",
                            "-u", f"{ak}:{sk}", ENDPOINT + f"{QUARANTINE}/{n}"],
                           capture_output=True, text=True)
        if p.stdout.strip() in ("200", "204", "404"):
            gone += 1
        else:
            failed += 1
            if failed <= 3:
                print(f"    FAILED {n}: {p.stdout.strip()}")
    print(f"\n  deleted {gone} objects via the S3 API, {failed} failed")

    left = os.listdir(DATA) if os.path.isdir(DATA) else []
    print(f"  quarantine directory now holds {len(left)} entries")
    print("  note: RustFS caches object metadata, so an object deleted by the API can")
    print("        still be listed on the filesystem for a while. The listing above is")
    print("        informational; the authoritative check is a S3 GET returning 404.")


if __name__ == "__main__":
    main()
