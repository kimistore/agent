#!/usr/bin/env python3
"""Where exactly does visibility stop, and does it stop at the same time for every series?

The claim is that data before a cutoff is missing, and that the cutoff differs
from metric to metric. Both are testable, and they are tested separately:

  sweep   an instant count(up) every few minutes across the boundary. If the
          answer stops at one time for everything, the boundary is a single
          event. If it stops at a different time per series, it is not a
          truncation.

  series  for a timestamp well before the boundary, which series still answer.
          A series that simply did not exist yet -- a pod created at 03:12, a
          counter that was introduced -- is not missing data, and the two look
          identical on a graph.
"""
import datetime
import json
import subprocess
import sys
import urllib.parse
import zoneinfo

TZ = zoneinfo.ZoneInfo("Europe/Amsterdam")
QUERIER = "http://127.0.0.1:9012/prometheus/api/v1"


def api(path, **params):
    url = f"{QUERIER}/{path}?" + urllib.parse.urlencode(params)
    out = subprocess.run(["curl", "-s", "--max-time", "120", url],
                         capture_output=True, text=True).stdout
    try:
        return json.loads(out)
    except Exception:
        return {"status": "unparseable", "raw": out[:200]}


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "sweep"
    day = sys.argv[2] if len(sys.argv) > 2 else "2026-10-14"

    if mode == "sweep":
        start = int(datetime.datetime.fromisoformat(f"{day}T00:00:00").replace(tzinfo=TZ).timestamp())
        end = int(datetime.datetime.fromisoformat(f"{day}T08:00:00").replace(tzinfo=TZ).timestamp())
        step = 300
        prev = None
        for ts in range(start, end, step):
            d = api("query", query="count(up)", time=ts)
            n = None
            try:
                r = d["data"]["result"]
                n = float(r[0]["value"][1]) if r else None
            except Exception:
                pass
            mark = ""
            if prev is not None and (n is None) != (prev is None):
                mark = "   <== boundary"
            print(f"  {datetime.datetime.fromtimestamp(ts, TZ).strftime('%H:%M')}  "
                  f"{'NO DATA' if n is None else int(n)}{mark}")
            prev = n

    elif mode == "series":
        at = int(datetime.datetime.fromisoformat(f"{day}T02:30:00").replace(tzinfo=TZ).timestamp())
        print(f"  asking which series answer at {day} 02:30 local")
        d = api("series", **{"match[]": "up", "start": at, "end": at + 300})
        rows = d.get("data") or []
        print(f"  series present then: {len(rows)}")
        for s in rows:
            print(f"    {s.get('job'):<40} {s.get('instance')}")

    elif mode == "latest":
        # For each series, when did it first appear over the last 48h?
        import time
        now = int(time.time())
        d = api("query", query="count(up)", time=now - 3600)
        r = d["data"]["result"]
        print(f"  now: {r[0]['value'][1] if r else 'NO DATA'} series up")
        d2 = api("query_range", query="count(up)", start=now - 48 * 3600,
                 end=now, step=600)
        res = d2.get("data", {}).get("result", [])
        if not res:
            print("  range query returned nothing")
            return
        pts = [(int(t), float(v)) for t, v in res[0]["values"] if float(v) > 0]
        if not pts:
            print("  no non-zero samples in 48h")
            return
        first = pts[0][0]
        print(f"  first non-zero sample: {datetime.datetime.fromtimestamp(first, TZ)}")
        gaps = []
        for i in range(len(pts) - 1):
            if pts[i + 1][0] - pts[i][0] > 1800:
                gaps.append((pts[i][0], pts[i + 1][0]))
        print(f"  gaps longer than 30 min inside the visible range: {len(gaps)}")
        for a, b in gaps[:8]:
            print(f"    {datetime.datetime.fromtimestamp(a, TZ).strftime('%m-%d %H:%M')} "
                  f".. {datetime.datetime.fromtimestamp(b, TZ).strftime('%m-%d %H:%M')}")


if __name__ == "__main__":
    main()
