#!/usr/bin/env python3
"""Decode real timestamps out of kimistore segments.

The index says which offsets exist; it says nothing about when they were
written. To answer "is the data from before 02:44 still here", the offsets have
to be turned back into wall-clock times by reading the records themselves.

A segment body is a sequence of message set entries, each laid out as:

    offset (8, BE) | size (4, BE) | message[size]

The message is either a legacy message or a Kafka 0.11+ RecordBatch. A
RecordBatch carries firstTimestamp at byte 35 and maxTimestamp at byte 43, both
big-endian. Producers differ in whether they wrap the batch in an outer message
set entry or emit it bare, so both are handled.
"""
import json
import os
import struct
import subprocess
import sys

ENVFILE = os.environ.get("KIMISTORE_ENV", "/home/andy/kimistore/kimistore.env")
PREFIX = "mimir-distributor/0"
ENDPOINT = "http://127.0.0.1:9000/kimistore/"
AK = SK = None


def read_env():
    global AK, SK
    for line in open(ENVFILE):
        if line.startswith("AWS_ACCESS_KEY_ID="):
            AK = line.split("=", 1)[1].strip()
        elif line.startswith("AWS_SECRET_ACCESS_KEY="):
            SK = line.split("=", 1)[1].strip()


def get(key, out):
    subprocess.run(["curl", "-s", "--aws-sigv4", "aws:amz:us-east-1:s3",
                    "-u", f"{AK}:{SK}", "-o", out, ENDPOINT + key],
                   capture_output=True, timeout=300)


def nm(r):
    n = "%020d" % r["b"]
    return n + ("-e%d" % r["e"] if r.get("e", 0) > 0 else "")


def batch_times(body):
    """Yield (offset, first_ts_ms) for each message in the segment body."""
    pos = 0
    n = len(body)
    while pos + 12 <= n:
        off, size = struct.unpack(">QI", body[pos:pos + 12])
        pos += 12
        if size <= 0 or pos + size > n:
            return
        msg = body[pos:pos + size]
        pos += size

        # A bare RecordBatch starts with its own baseOffset and batchLength.
        # A wrapped one starts with the same, because the wrapper is stripped
        # above; either way, byte 16 is the magic byte and must be 2.
        if len(msg) > 51 and msg[16] == 2:
            first_ts = struct.unpack(">q", msg[35:43])[0]
            max_ts = struct.unpack(">q", msg[43:51])[0]
            if 0 < first_ts < 4_000_000_000_000:      # plausible ms, not garbage
                yield off, first_ts, max_ts
        elif len(msg) > 12 and msg[4] in (0, 1):     # legacy message
            ts = struct.unpack(">q", msg[0:8])[0]
            if 0 < ts < 4_000_000_000_000:
                yield off, ts, ts


def local(ts_ms):
    import datetime, zoneinfo
    tz = zoneinfo.ZoneInfo("Europe/Amsterdam")
    return datetime.datetime.fromtimestamp(ts_ms / 1000, tz).strftime("%Y-%m-%d %H:%M:%S")


def main():
    read_env()
    get("_agents/knode3/checkpoint.json", "/tmp/tc-cp.json")
    d = json.load(open("/tmp/tc-cp.json"))
    pv = d["topics"]["mimir-distributor"]["partitions"]["0"]
    sealed = pv.get("sealed") or []
    lo = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    hi = int(sys.argv[2]) if len(sys.argv) > 2 else 1 << 62

    print(f"  log_start_offset {pv['log_start_offset']}  leo {pv['log_end_offset']}")
    rows = []
    for b in sealed:
        get(b["key"], "/tmp/tc-b.json")
        try:
            refs = json.load(open("/tmp/tc-b.json"))
        except Exception:
            continue
        for r in sorted(refs, key=lambda x: x["b"]):
            if not (lo <= r["b"] <= hi):
                continue
            get(f"{PREFIX}/{nm(r)}.log", "/tmp/tc-s.log")
            try:
                body = open("/tmp/tc-s.log", "rb").read()
            except OSError:
                rows.append((r["b"], None, None, "UNREADABLE"))
                continue
            times = list(batch_times(body))
            if not times:
                rows.append((r["b"], None, None, f"no batches ({len(body)}B)"))
                continue
            f = min(t[1] for t in times)
            m = max(t[2] for t in times)
            rows.append((r["b"], f, m, f"{len(times)} batches"))

    for base, f, m, note in rows:
        if f is None:
            print(f"  {base:>10}  {note}")
        else:
            print(f"  {base:>10}  {local(f)}  ..  {local(m)}   {note}")
    if rows:
        print(f"\n  covered {local(rows[0][1])} .. {local(rows[-1][2])}")


if __name__ == "__main__":
    main()
