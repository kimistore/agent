#!/usr/bin/env python3
"""What time range does each trial-bucket block actually cover, and does it hold data?

Block creation time and block coverage are different things. Mimir names a block
with a ULID stamped at creation, but the data inside spans whatever window the
ingester held. Reading only the ULID says when a block appeared; reading meta.json
says what it can answer for. Those two disagree here, so both are printed.
"""
import datetime
import json
import os
import subprocess
import sys
import zoneinfo

TZ = zoneinfo.ZoneInfo("Europe/Amsterdam")
AL = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
ENVFILE = os.environ.get("KIMISTORE_ENV", "/home/andy/kimistore/kimistore.env")
BUCKET = "trial-mimir-blocks"
DIR = f"/home/andy/RustFS/data/{BUCKET}/anonymous"


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


def stamp(ms):
    return datetime.datetime.fromtimestamp(ms / 1000, TZ).strftime("%m-%d %H:%M")


def main():
    how_many = int(sys.argv[1]) if len(sys.argv) > 1 else 12
    ak, sk = creds()
    names = [n.strip() for n in os.listdir(DIR) if len(n.strip()) >= 26 and n.strip() not in ("chunks", "index")]
    names = [n for n in names if all(c in AL for c in n[:10])]
    names.sort(key=ulid_ts)

    print(f"  {len(names)} blocks in {BUCKET}")
    print(f"  {'created':<12} {'covers from':<12} {'covers to':<12} {'samples':>10}  flags")
    for n in names[-how_many:]:
        created = stamp(ulid_ts(n))
        subprocess.run(["curl", "-s", "--aws-sigv4", "aws:amz:us-east-1:s3",
                        "-u", f"{ak}:{sk}", "-o", "/tmp/bmeta.json",
                        f"http://127.0.0.1:9000/{BUCKET}/anonymous/{n}/meta.json"],
                       capture_output=True)
        flags = []
        if os.path.isdir(os.path.join(DIR, n, "deletion-mark.json")):
            flags.append("DELETED")
        try:
            d = json.load(open("/tmp/bmeta.json"))
            samples = d.get("stats", {}).get("samples")
            print(f"  {created:<12} {stamp(d.get('minTime', 0)):<12} "
                  f"{stamp(d.get('maxTime', 0)):<12} {str(samples):>10}  {' '.join(flags)}")
        except Exception as exc:
            print(f"  {created:<12} {'?':<12} {'?':<12} {'?':>10}  meta unreadable: {exc} {' '.join(flags)}")

    # Does the coverage of consecutive blocks actually join up?
    spans = []
    for n in names:
        subprocess.run(["curl", "-s", "--aws-sigv4", "aws:amz:us-east-1:s3",
                        "-u", f"{ak}:{sk}", "-o", "/tmp/bmeta.json",
                        f"http://127.0.0.1:9000/{BUCKET}/anonymous/{n}/meta.json"],
                       capture_output=True)
        try:
            d = json.load(open("/tmp/bmeta.json"))
            spans.append((d.get("minTime", 0), d.get("maxTime", 0)))
        except Exception:
            pass
    spans.sort()
    gaps = [(spans[i][1], spans[i + 1][0]) for i in range(len(spans) - 1)
            if spans[i + 1][0] - spans[i][1] > 60_000]
    print(f"\n  coverage gaps over {len(spans)} blocks: {len(gaps)}")
    for a, b in gaps[:6]:
        print(f"    {stamp(a)} .. {stamp(b)}  ({(b - a) / 3600000:.1f} h)")


if __name__ == "__main__":
    main()
