#!/usr/bin/env python3
"""Fill in the panel fields Grafana's Scenes engine requires.

Grafana 13 renders dashboards through Scenes, which is stricter than the older
dashboard model. A panel missing `options` or `fieldConfig.defaults` is tolerated
by some versions and rejected by others; in this one it surfaces as a generic
"An unexpected error happened" in the browser, with nothing in the server log
and every query returning status=ok.

The symptom is the same shape as a bad layout, which is why both were wrong at
once and the layout fix alone did not make the dashboard open. A defect that only
appears in the browser is not diagnosable from query results, so the structural
check is as much a part of the fix as the fields themselves.

Existing values are preserved. This only adds what is missing, so it can be run
repeatedly over a dashboard that has been hand-edited.
"""
import json
import sys

STAT_OPTIONS = {
    "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
    "orientation": "auto",
    "textMode": "auto",
    "colorMode": "value",
    "graphMode": "area",
    "justifyMode": "auto",
    "wideLayout": True,
}

TIMESERIES_OPTIONS = {
    "legend": {
        "displayMode": "list",
        "placement": "bottom",
        "calcs": [],
        "showLegend": True,
    },
    "tooltip": {"mode": "multi", "sort": "none"},
}

ROW_OPTIONS = {"showTitle": True, "collapsed": False}

UNITS = {"timeseries": "short", "stat": "short"}


def deep_fill(target, defaults):
    """Add missing keys recursively; never overwrite what is already there."""
    for k, v in defaults.items():
        if k not in target:
            target[k] = json.loads(json.dumps(v))
        elif isinstance(v, dict) and isinstance(target[k], dict):
            deep_fill(target[k], v)


def main():
    path = sys.argv[1]
    d = json.load(open(path))
    fixed = []

    for p in d.get("panels", []):
        kind = p.get("type")
        if kind == "row":
            p.setdefault("collapsed", False)
            p.setdefault("panels", [])
            continue

        fc = p.setdefault("fieldConfig", {})
        deep_fill(fc, {"defaults": {"unit": UNITS.get(kind, "short"), "custom": {}}, "overrides": []})
        if kind == "stat":
            deep_fill(fc["defaults"], {"mappings": [], "thresholds": {"mode": "absolute", "steps": [{"color": "green"}]}})
        if kind == "timeseries":
            deep_fill(fc["defaults"], {"custom": {
                "drawStyle": "line",
                "lineWidth": 1,
                "fillOpacity": 8,
                "showPoints": "never",
                "spanNulls": False,
                "pointSize": 5,
                "axisPlacement": "auto",
                "gradientMode": "none",
            }})

        opts = p.setdefault("options", {})
        deep_fill(opts, STAT_OPTIONS if kind == "stat" else TIMESERIES_OPTIONS if kind == "timeseries" else {})

        p.setdefault("transparent", False)
        p.setdefault("description", p.get("description", ""))
        p.setdefault("interval", None)
        p.setdefault("links", [])
        fixed.append(f"{kind}:{p.get('title')}")

    json.dump(d, open(path, "w"), indent=2)
    print(f"  filled required fields on {len(fixed)} panels")


if __name__ == "__main__":
    main()
