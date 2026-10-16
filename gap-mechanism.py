#!/usr/bin/env python3
"""Why the wedge's data is not coming back, and whether any of it can.

compare-mimirs.py showed the gap is static and probe-gap.py showed it is a real
absence rather than a sampling artefact. This narrows that to the mechanism.

The broker assigned offsets to every rejected produce and the objects are in
object storage, so the records exist. The question is whether Mimir's ingest
consumer ever read them. It reports a committed offset, so this compares three
things at the same instants:

  - what production holds,
  - what the trial holds,
  - whether the trial's coverage is total or merely sparse in the gap window.

If the trial is sparse rather than empty, some records survived the retries and
only the rest are unrecoverable -- a materially different answer from "all of
it is gone".
"""
import datetime
import json
import subprocess
import urllib.parse
import zoneinfo

TZ = zoneinfo.ZoneInfo("Europe/Amsterdam")
TRIAL = "http://127.0.0.1:9012/prometheus/api/v1"
PROD = "http://127.0.0.1:9009/prometheus/api/v1"

START = datetime.datetime(2026, 10, 16, 6, 0, tzinfo=datetime.timezone.utc)
END = datetime.datetime(2026, 10, 16, 9, 0, tzinfo=datetime.timezone.utc)
STEP = 60


def range_query(base, q, start, end, step):
    url = base + "/query_range?" + urllib.parse.urlencode(
        {"query": q, "start": int(start), "end": int(end), "step": step})
    out = subprocess.run(["curl", "-s", "--max-time", "120", url],
                         capture_output=True, text=True).stdout
    try:
        return json.loads(out)
    except Exception:
        return {}


def timestamps_with_samples(base):
    """Set of step timestamps at which at least one series carried a value."""
    d = range_query(base, 'up{job="node-exporter"}', START.timestamp(), END.timestamp(), STEP)
    present = set()
    for s in d.get("data", {}).get("result", []):
        for ts, v in s.get("values", []):
            if v not in ("0", "NaN"):
                present.add(int(ts))
    return present


def main():
    all_steps = list(range(int(START.timestamp()), int(END.timestamp()) + STEP, STEP))
    prod = timestamps_with_samples(PROD)
    trial = timestamps_with_samples(TRIAL)

    print(f"  window 06:00-09:00 UTC ({len(all_steps)} x {STEP}s steps)")
    print(f"    production has samples at : {len(prod)} steps")
    print(f"    trial     has samples at : {len(trial)} steps")
    overlap = prod & trial
    print(f"    both                     : {len(overlap)} steps")
    if prod:
        print(f"    trial coverage of prod   : {100.0 * len(overlap) / len(prod):.1f}%")

    # Where does the trial's coverage stop and resume? A clean edge means the
    # consumer skipped a contiguous offset range; scattered islands mean the
    # retries that succeeded are interleaved with the ones that did not.
    only_prod = sorted(prod - trial)
    print(f"\n  --- trial misses {len(only_prod)} steps production has ---")
    if only_prod:
        runs, cur = [], [only_prod[0]]
        for t in only_prod[1:]:
            if t - cur[-1] <= STEP * 2:
                cur.append(t)
            else:
                runs.append(cur)
                cur = [t]
        runs.append(cur)
        for r in runs[:12]:
            a = datetime.datetime.fromtimestamp(r[0], datetime.timezone.utc)
            b = datetime.datetime.fromtimestamp(r[-1] + STEP, datetime.timezone.utc)
            print(f"    {a:%H:%M} .. {b:%H:%M} UTC   {len(r):>3} steps")
        if len(runs) > 12:
            print(f"    ... and {len(runs) - 12} more runs")
        longest = max(runs, key=len)
        print(f"\n    longest continuous miss: {len(longest)} steps "
              f"({len(longest)} min) starting "
              f"{datetime.datetime.fromtimestamp(longest[0], datetime.timezone.utc):%H:%M} UTC")


if __name__ == "__main__":
    main()