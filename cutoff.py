#!/usr/bin/env python3
"""Per-series earliest available sample, queried straight from the Mimir querier.

The claim under test is that the visible cutoff differs from metric to metric. A
time-based truncation would give every series the same boundary, so what is being
measured here is the distribution: for each job, the earliest sample that comes
back and the latest, over a range wide enough to contain the suspected cutoff.

Status, warnings and errors are printed as well as results, because an empty
result and a refused query look identical if you only look at the data.
"""
import datetime
import json
import subprocess
import sys
import urllib.parse

QUERIER = "http://127.0.0.1:9012/prometheus/api/v1"
TZ = datetime.timezone(datetime.timedelta(hours=2))     # Europe/Amsterdam, CEST


def api(path, **params):
    url = f"{QUERIER}/{path}?" + urllib.parse.urlencode(params)
    out = subprocess.run(["curl", "-s", "--max-time", "180", url],
                         capture_output=True, text=True).stdout
    try:
        return json.loads(out)
    except Exception:
        return {"status": "unparseable", "raw": out[:300]}


def stamp(ts):
    return datetime.datetime.fromtimestamp(int(ts), TZ).strftime("%m-%d %H:%M")


def main():
    hours = int(sys.argv[1]) if len(sys.argv) > 1 else 24
    import time
    now = int(time.time())
    start = now - hours * 3600
    print(f"  window: {stamp(start)} .. {stamp(now)} local ({hours}h)")

    # Which jobs exist at all right now?
    d = api("series", **{"match[]": "up", "start": now - 3600, "end": now})
    jobs = sorted({s["job"] for s in (d.get("data") or [])}) if d.get("status") == "success" else []
    print(f"  jobs currently scraped: {jobs}\n")

    for job in jobs:
        d = api("query_range", **{
            "query": f'count_over_time(up{{job="{job}"}}[5m])',
            "start": start, "end": now, "step": 600,
        })
        if d.get("status") != "success":
            print(f"  {job:22} QUERY REFUSED: {str(d.get('error'))[:120]}")
            continue
        result = d.get("data", {}).get("result", [])
        if not result:
            print(f"  {job:22} no data in the whole window")
            continue
        rows = []
        for s in result:
            pts = [(t, v) for t, v in s["values"] if v not in ("0", "NaN")]
            if pts:
                rows.append((s["metric"].get("instance", "?"), pts))
        if not rows:
            print(f"  {job:22} {len(result)} series but every step is empty")
            continue
        first = min(p[0][0] for _, p in rows)
        last = max(p[-1][0] for _, p in rows)
        total_steps = sum(len(s["values"]) for s in result)
        empty = total_steps - sum(len(p) for _, p in rows)
        print(f"  {job:22} series={len(rows):<3} first={stamp(first)}  "
              f"last={stamp(last)}  empty_steps={empty}")
        if d.get("warnings"):
            print(f"  {'':22} WARNINGS: {d['warnings'][:200]}")


if __name__ == "__main__":
    main()
