#!/usr/bin/env python3
"""How much data did the wedged durability frontier cost the trial Mimir?

The production Mimir and the trial receive the same remote-write stream: Alloy
exports to both, one direct and one through kimistore. So comparing them over the
same window isolates what the broker path lost, rather than what the cluster
failed to emit.

Gaps are found by asking whether a scrape-target's `up` series has a sample in
each five-minute bucket. A bucket where production has samples and the trial has
none is a gap the broker path introduced.
"""
import datetime
import json
import subprocess
import urllib.parse
import zoneinfo

TZ = zoneinfo.ZoneInfo("Europe/Amsterdam")
TRIAL = "http://127.0.0.1:9012/prometheus/api/v1"
PROD = "http://127.0.0.1:9009/prometheus/api/v1"
JOB = "node-exporter"
HOURS = 14
BUCKET = 300


def query(base, q, start, end, step):
    url = f"{base}/query_range?" + urllib.parse.urlencode(
        {"query": q, "start": int(start), "end": int(end), "step": step})
    out = subprocess.run(["curl", "-s", "--max-time", "120", url],
                         capture_output=True, text=True).stdout
    try:
        return json.loads(out)
    except Exception:
        return {}


def covered(base, start, end, step):
    """Set of timestamps at which this instance had a sample."""
    d = query(base, f'up{{job="{JOB}"}}', start, end, step)
    res = d.get("data", {}).get("result", [])
    pts = set()
    for s in res:
        for ts, v in s.get("values", []):
            if v not in ("0", "NaN"):
                pts.add(int(ts))
    return pts


def main():
    now = int(datetime.datetime.now(TZ).timestamp())
    start = now - HOURS * 3600
    step = BUCKET

    prod = covered(PROD, start, now, step)
    trial = covered(TRIAL, start, now, step)
    print(f"  window: last {HOURS}h, {BUCKET}s buckets, job={JOB}")
    print(f"    production samples: {len(prod)}")
    print(f"    trial samples    : {len(trial)}")
    if not prod:
        print("  production returned nothing; comparison is not meaningful")
        return

    missing = sorted(prod - trial)
    extra = sorted(trial - prod)
    print(f"    in production but NOT trial : {len(missing)} buckets")
    print(f"    in trial but NOT production : {len(extra)} buckets")

    if missing:
        print("\n  --- contiguous gaps (local time) ---")
        groups = []
        cur = [missing[0]]
        for t in missing[1:]:
            if t - cur[-1] <= BUCKET * 2:
                cur.append(t)
            else:
                groups.append(cur)
                cur = [t]
        groups.append(cur)
        for g in groups:
            a = datetime.datetime.fromtimestamp(g[0], TZ)
            b = datetime.datetime.fromtimestamp(g[-1] + BUCKET, TZ)
            dur = (g[-1] + BUCKET - g[0]) / 60
            print(f"    {a:%m-%d %H:%M} .. {b:%m-%d %H:%M}   {len(g) * BUCKET / 60:.0f} min")
        total = sum((g[-1] + BUCKET - g[0]) for g in groups) / 60
        print(f"    TOTAL missing: {total:.0f} min over {len(groups)} gap(s)")


if __name__ == "__main__":
    main()
