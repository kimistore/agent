#!/usr/bin/env python3
"""Are the blocks that cover the invisible window actually empty?

Mimir has blocks spanning every hour of today, yet a query for 03:00 returns
nothing. A block that exists but holds no chunks would explain that: the query
path finds the block, reads it, and finds no samples.

So this reports each block's real byte count on disk, not its name and not its
ULID timestamp. Directory size is measured from the filesystem because that is
what RustFS actually stores; the S3 API would have to list every chunk.
"""
import datetime
import json
import os
import subprocess
import sys
import zoneinfo

TZ = zoneinfo.ZoneInfo("Europe/Amsterdam")
AL = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
BUCKET = "trial-mimir-blocks"
D = f"/home/andy/RustFS/data/{BUCKET}/anonymous"
ENVFILE = os.environ.get("KIMISTORE_ENV", "/home/andy/kimistore/kimistore.env")


def creds():
    ak = sk = None
    for line in open(ENVFILE):
        if line.startswith("AWS_ACCESS_KEY_ID="):
            ak = line.split("=", 1)[1].strip()
        elif line.startswith("AWS_SECRET_ACCESS_KEY="):
            sk = line.split("=", 1)[1].strip()
    return ak, sk


def ulid_ts(u):
    v = 0
    for ch in u[:10]:
        v = v * 32 + AL.index(ch)
    return v


def dirsize(path):
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


def stamp(ms):
    return datetime.datetime.fromtimestamp(ms / 1000, TZ).strftime("%m-%d %H:%M")


def main():
    lo_h = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    hi_h = int(sys.argv[2]) if len(sys.argv) > 2 else 6
    ak, sk = creds()
    today = datetime.datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    lo = int((today + datetime.timedelta(hours=lo_h)).timestamp() * 1000)
    hi = int((today + datetime.timedelta(hours=hi_h)).timestamp() * 1000)

    names = [n for n in os.listdir(D) if len(n) >= 26 and all(c in AL for c in n[:10])]
    names.sort(key=ulid_ts)

    print(f"  blocks overlapping {stamp(lo)} .. {stamp(hi)}")
    print(f"  {'block':<15} {'covers':<16} {'chunk bytes':>14}  {'deleted':<8} samples")
    empty = 0
    shown = 0
    for n in names:
        subprocess.run(["curl", "-s", "--aws-sigv4", "aws:amz:us-east-1:s3",
                        "-u", f"{ak}:{sk}", "-o", "/tmp/bm.json",
                        f"http://127.0.0.1:9000/{BUCKET}/anonymous/{n}/meta.json"],
                       capture_output=True)
        try:
            d = json.load(open("/tmp/bm.json"))
        except Exception:
            continue
        mn, mx = d.get("minTime", 0), d.get("maxTime", 0)
        if mx < lo or mn > hi:
            continue
        size = dirsize(os.path.join(D, n, "chunks"))
        deleted = "YES" if os.path.isdir(os.path.join(D, n, "deletion-mark.json")) else "no"
        st = d.get("stats", {})
        samples = st.get("samplesNum", st.get("samples"))
        if size == 0:
            empty += 1
        shown += 1
        print(f"  {n[:14]:<15} {stamp(mn)}..{stamp(mx)}  {size:>14,}  {deleted:<8} {samples}")

    print(f"\n  {shown} blocks in range, {empty} of them empty")


if __name__ == "__main__":
    main()
