#!/usr/bin/env python3
"""Are the trial Mimir's gaps real, or an artifact of 5m-bucket sampling?

compare-mimirs.py inferred gaps from which 300s buckets had a sample. That is
good for locating a window but weak as proof, because a single missed bucket and
a genuinely absent range look identical at that resolution.

This asks the direct question instead: at a chosen instant, does the trial hold
series that production holds? An instant query returns what the store actually
has at that timestamp, so an empty result is a real absence rather than a
sampling artefact.
"""
import datetime
import json
import subprocess
import urllib.parse
import zoneinfo

TZ = zoneinfo.ZoneInfo("Europe/Amsterdam")
TRIAL = "http://127.0.0.1:9012/prometheus/api/v1"
PROD = "http://127.0.0.1:9009/prometheus/api/v1"


def query(base, q, ts):
    url = base + "/query?" + urllib.parse.urlencode({"query": q, "time": int(ts)})
    out = subprocess.run(["curl", "-s", "--max-time", "60", url],
                         capture_output=True, text=True).stdout
    try:
        return json.loads(out)
    except Exception:
        return {}


def series_at(base, ts):
    return len(query(base, 'up{job="node-exporter"}', ts)
               .get("data", {}).get("result", []))


def main():
    # Probes either side of the wedge: deep inside it, at the recovery boundary,
    # and live now. Written in UTC because that is what the broker logs use.
    probes = [
        ("before the wedge", "2026-10-16 00:00"),
        ("deep in a gap", "2026-10-16 07:05"),
        ("late in a gap", "2026-10-16 08:40"),
        ("just after restart", "2026-10-16 11:00"),
        ("right now", "2026-10-16 13:10"),
    ]
    print(f"  {'when (UTC)':<18} {'local':<8} {'production':>11} {'trial':>7}   verdict")
    for label, when in probes:
        ts = datetime.datetime.strptime(when, "%Y-%m-%d %H:%M").replace(
            tzinfo=datetime.timezone.utc).timestamp()
        n_prod, n_trial = series_at(PROD, ts), series_at(TRIAL, ts)
        local = datetime.datetime.fromtimestamp(ts, TZ).strftime("%H:%M")
        if n_prod and not n_trial:
            verdict = "REAL GAP"
        elif n_prod and n_trial:
            verdict = "both present"
        elif not n_prod and not n_trial:
            verdict = "absent from both"
        else:
            verdict = "trial-only (unexpected)"
        print(f"  {when:<18} {local:<8} {n_prod:>11} {n_trial:>7}   {verdict}  [{label}]")


if __name__ == "__main__":
    main()