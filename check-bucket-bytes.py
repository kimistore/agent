#!/usr/bin/env python3
"""Does BucketRef.Bytes actually equal the sum of the segment sizes in the bucket?

Retention is policy-driven: reclaimByPolicy walks the sealed buckets, sums Bytes,
and deletes once the accumulated total passes the budget. If Bytes under-reports,
retention keeps far more than intended for the same configured budget. So this
compares the recorded value against the sum of the sizes the bucket actually
lists, bucket by bucket.

An earlier observation that made this worth checking: a bucket's Bytes was
exactly equal to its last segment's size, which would be the case if Bytes were
being taken from a single entry rather than accumulated.
"""
import json
import os
import subprocess
import sys

ENVFILE = os.environ.get("KIMISTORE_ENV", "/home/andy/kimistore/kimistore.env")
ENDPOINT = "http://127.0.0.1:9000/kimistore/"


def creds():
    ak = sk = None
    for line in open(ENVFILE):
        if line.startswith("AWS_ACCESS_KEY_ID="):
            ak = line.split("=", 1)[1].strip()
        elif line.startswith("AWS_SECRET_ACCESS_KEY="):
            sk = line.split("=", 1)[1].strip()
    return ak, sk


def get(key, out):
    ak, sk = creds()
    subprocess.run(["curl", "-s", "--aws-sigv4", "aws:amz:us-east-1:s3",
                    "-u", f"{ak}:{sk}", "-o", out, ENDPOINT + key],
                   capture_output=True, timeout=300)


def main():
    get("_agents/knode3/checkpoint.json", "/tmp/bc-cp.json")
    d = json.load(open("/tmp/bc-cp.json"))
    pv = d["topics"]["mimir-distributor"]["partitions"]["0"]
    sealed = pv.get("sealed") or []

    total_recorded = 0
    total_actual = 0
    worst = []
    for b in sealed:
        get(b["key"], "/tmp/bc-b.json")
        try:
            refs = json.load(open("/tmp/bc-b.json"))
        except Exception:
            continue
        actual = sum(r.get("s", 0) for r in refs)
        recorded = b.get("bytes", 0)
        total_recorded += recorded
        total_actual += actual
        if actual and recorded != actual:
            ratio = recorded / actual
            worst.append((abs(recorded - actual), b["key"], len(refs), recorded, actual, ratio))

    worst.sort(reverse=True)
    gb = 1e9
    print(f"  buckets: {len(sealed)}")
    print(f"  recorded Bytes (sum over buckets): {total_recorded/gb:8.3f} GB")
    print(f"  actual sum of segment sizes      : {total_actual/gb:8.3f} GB")
    if total_actual:
        print(f"  recorded / actual                : {total_recorded/total_actual:8.3f}")
    print(f"  buckets where Bytes != sum       : {len(worst)} of {len(sealed)}")
    for diff, key, n, rec, act, ratio in worst[:6]:
        print(f"    {key[-28:]}  entries={n:<4} recorded={rec/1e6:8.2f} MB  "
              f"actual={act/1e6:8.2f} MB  ratio={ratio:.3f}")


if __name__ == "__main__":
    main()
