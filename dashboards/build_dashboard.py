#!/usr/bin/env python3
"""Generate the kimistore dashboard.

Built from the single-panel shape that is known to load, rather than edited into
the larger file that did not. Every panel field is written out explicitly:
options, fieldConfig.defaults, fieldConfig.overrides, gridPos, and the datasource
on both the panel and the target.

The explicit-everything rule is the point. Two of the failures that preceded this
were fields left for Grafana to fill in, and a defect that only appears in the
browser cannot be diagnosed from what the server logged -- so anything not
written here is something that will not be checked.

Layout is computed, not hand-placed. The earlier 24-panel version had three
panels at the same y with a 24-wide one on top of two 12-wide ones, because a
hand-shift moved rows down and then reset the siblings back. Wrapping at 24
columns and asserting no two panels share a cell removes that class entirely.
"""
import json
import sys

DS = {"type": "prometheus", "uid": "dg0i3veoyn5z4b"}
COLUMNS = 24

STAT_DEFAULTS = {
    "unit": "short",
    "mappings": [],
    "thresholds": {"mode": "absolute", "steps": [{"color": "green"}]},
}

TS_DEFAULTS = {
    "unit": "short",
    "mappings": [],
    "thresholds": {"mode": "absolute", "steps": [{"color": "green"}]},
    "custom": {
        "drawStyle": "line",
        "lineWidth": 1,
        "fillOpacity": 10,
        "showPoints": "never",
        "spanNulls": False,
        "pointSize": 5,
        "axisPlacement": "auto",
        "gradientMode": "none",
        "stacking": {"mode": "none", "group": "A"},
        "hideFrom": {"legend": False, "tooltip": False, "viz": False},
    },
}

STAT_OPTIONS = {
    "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
    "orientation": "auto",
    "textMode": "auto",
    "colorMode": "value",
    "graphMode": "area",
    "justifyMode": "auto",
    "wideLayout": True,
}

TS_OPTIONS = {
    "legend": {"displayMode": "list", "placement": "bottom", "calcs": [], "showLegend": True},
    "tooltip": {"mode": "multi", "sort": "none"},
}

_pid = [0]


def ref_ids(n):
    """A, B, C ... then AA, AB. The error-rates panel has more than 26 series."""
    out = []
    for i in range(n):
        s = ""
        i += 1
        while i:
            i, r = divmod(i - 1, 26)
            s = chr(ord("A") + r) + s
        out.append(s)
    return out


def panel(kind, title, description, targets, defaults, options, instant=False, uid_filters=True):
    _pid[0] += 1
    ts = []
    for rid, (expr, legend) in zip(ref_ids(len(targets)), targets):
        ts.append({
            "refId": rid,
            "datasource": DS,
            "editorMode": "code",
            "expr": expr,
            "legendFormat": legend,
            "instant": instant,
            "range": not instant,
        })
    return {
        "id": _pid[0],
        "type": kind,
        "title": title,
        "description": description,
        "datasource": DS,
        "targets": ts,
        "options": json.loads(json.dumps(options)),
        "fieldConfig": {
            "defaults": json.loads(json.dumps(defaults)),
            "overrides": [],
        },
    }


def stat(title, description, targets, **over):
    d = json.loads(json.dumps(STAT_DEFAULTS))
    d.update(over)
    return panel("stat", title, description, targets, d, STAT_OPTIONS, instant=True)


def ts(title, description, targets, unit="short"):
    d = json.loads(json.dumps(TS_DEFAULTS))
    d["unit"] = unit
    return panel("timeseries", title, description, targets, d, TS_OPTIONS)


def row(title):
    _pid[0] += 1
    return {
        "id": _pid[0],
        "type": "row",
        "title": title,
        "collapsed": False,
        "panels": [],
        "gridPos": {"h": 1, "w": COLUMNS, "x": 0, "y": 0},
    }


def layout(panels):
    """Place panels left to right, wrapping at 24 columns, and prove no overlap."""
    runs, run = [], []
    for p in panels:
        if p["type"] == "timeseries":
            run.append(p)
        else:
            if run:
                runs.append(run)
            run = []
    if run:
        runs.append(run)
    for group in runs:
        per_row = min(len(group), 2)
        w = COLUMNS // per_row
        for p in group:
            p["_w"] = w

    y = x = band = 0
    for p in panels:
        if p["type"] == "row":
            if x:
                y += band
                x = 0
            p["gridPos"] = {"h": 1, "w": COLUMNS, "x": 0, "y": y}
            y += 1
            band = 0
            continue
        w = p.pop("_w", 6 if p["type"] == "stat" else 12)
        h = 4 if p["type"] == "stat" else 7
        if x + w > COLUMNS:
            y += band
            x = 0
            band = 0
        p["gridPos"] = {"h": h, "w": w, "x": x, "y": y}
        x += w
        band = max(band, h)
    return panels


def assert_no_overlap(panels):
    cells = {}
    for p in panels:
        g = p["gridPos"]
        for dy in range(g["h"]):
            for dx in range(g["w"]):
                k = (g["y"] + dy, g["x"] + dx)
                cells.setdefault(k, []).append(p["title"])
    bad = {k: v for k, v in cells.items() if len(v) > 1}
    if bad:
        for k, v in list(bad.items())[:5]:
            print(f"    OVERLAP y={k[0]} x={k[1]}: {' <-> '.join(sorted(set(v)))}", file=sys.stderr)
        sys.exit("layout has overlapping panels")


def build():
    live = "max(kimistore_agent_live)"
    p = []

    p.append(row("Health"))

    p.append(stat(
        "Agent live",
        "1 when the agent reports itself live. Anything else means the process is up but not serving.",
        [(live, "live")],
        mappings=[{"type": "value", "options": {
            "0": {"text": "DOWN", "color": "red"},
            "1": {"text": "UP", "color": "green"}}}],
        thresholds={"mode": "absolute", "steps": [
            {"color": "red", "value": None}, {"color": "green", "value": 1}]},
    ))

    p.append(stat(
        "Partitions owned",
        "Partitions this agent holds. A shortfall against the cluster's count means ownership is not settled.",
        [("sum(kimistore_partitions_owned)", "owned")],
        colorMode="none",
    ))

    p.append(stat(
        "Uncommitted merged segments",
        "Written minus committed for the compactor. Zero in steady state; a value that only rises is a merge "
        "that wrote a segment and never committed it, which nothing else will ever collect.",
        [("max(kimistore_compactor_uncommitted_merged_segments)", "uncommitted")],
        thresholds={"mode": "absolute", "steps": [
            {"color": "green", "value": None}, {"color": "red", "value": 1}]},
    ))

    p.append(stat(
        "Compactor idle",
        "Seconds since the last merge that consolidated anything. A large value while the pass rate is flat "
        "means the compactor has stopped -- the failure that once ran silently for eight hours. Shows nothing "
        "until the first merge, because last_merge is 0 until then and time() minus 0 is the unix epoch.",
        [("(time() - max(kimistore_compactor_last_merge_timestamp_seconds)) and "
          "(max(kimistore_compactor_last_merge_timestamp_seconds) > 0)", "idle")],
        unit="s",
    ))

    p.append(ts(
        "Compactor: passes vs merges",
        "The liveness signal. Passes climbing with merges flat is a compactor running and idle; passes flat is "
        "one that has stopped. Split across two panels those are indistinguishable, which is what let the "
        "compactor sit broken for eight hours while every log line looked normal.",
        [("sum(rate(kimistore_compactor_passes_total[5m]))", "passes"),
         ("sum(rate(kimistore_compactor_merges_total[5m]))", "merges"),
         ("sum(rate(kimistore_compactor_merge_commits_total[5m]))", "commits")],
    ))

    p.append(ts(
        "Error rates",
        "Everything that should stay flat. Any sustained non-zero line is a fault, not background noise. The "
        "refused-write counters are here too: a refused write means something tried to write a partition it does "
        "not own, which is the failure that would corrupt the log if the fence were not holding.",
        [("sum(rate(kimistore_offset_flush_failures_total[5m]))", "offset flush"),
         ("sum(rate(kimistore_ownership_claim_failures_total[5m]))", "ownership claim"),
         ("sum(rate(kimistore_ownership_renewal_failures_total[5m]))", "ownership renewal"),
         ("sum(rate(kimistore_lease_renewal_failures_total[5m]))", "lease renewal"),
         ("sum(rate(kimistore_durable_timeouts_total[5m]))", "durable timeout"),
         ("sum(rate(kimistore_object_store_timeouts_total[5m]))", "object store timeout"),
         ("sum(rate(kimistore_wal_recovery_truncations_total[5m]))", "wal truncation"),
         ("sum(rate(kimistore_compactor_pass_errors_total[5m]))", "compactor pass"),
         ("sum(rate(kimistore_lease_refused_writes_total[5m]))", "lease refused writes"),
         ("sum(rate(kimistore_ownership_refused_writes_total[5m]))", "ownership refused writes"),
         ("sum(rate(kimistore_manifest_writes_rejected_total[5m]))", "manifest write rejected")],
    ))

    p.append(row("Ingest"))

    p.append(ts(
        "Request rate and throughput",
        "What is arriving, and the request body size behind it.",
        [("sum(rate(kimistore_request_count_total[5m]))", "requests"),
         ("sum(rate(kimistore_request_size_bytes_total[5m]))", "bytes in"),
         ("sum(rate(kimistore_uploader_bytes_total[5m]))", "bytes uploaded")],
    ))

    p.append(ts(
        "Request latency",
        "p50/p95/p99 from the request-time histogram. A histogram exposes _bucket/_sum/_count, so the bare name "
        "returns nothing; and a mean hides exactly the tail that makes a broker slow. With a 1s flush interval "
        "the acks=all tail is dominated by waiting for the next flush, which is what the interval is specified "
        "to do -- so p99 is a knob (KIMISTORE_FLUSH_INTERVAL_MS), not a defect.",
        [("histogram_quantile(0.50, sum by (le) (rate(kimistore_request_time_milliseconds_total_bucket[5m])))", "p50"),
         ("histogram_quantile(0.95, sum by (le) (rate(kimistore_request_time_milliseconds_total_bucket[5m])))", "p95"),
         ("histogram_quantile(0.99, sum by (le) (rate(kimistore_request_time_milliseconds_total_bucket[5m])))", "p99")],
        unit="ms",
    ))

    p.append(ts(
        "Uploader",
        "Segments in flight and work dropped. A queue that climbs without draining is backpressure on the "
        "object store; missed events mean the uploader dropped work.",
        [("max(kimistore_uploader_in_flight_count)", "in flight"),
         ("sum(rate(kimistore_uploader_task_total[5m]))", "tasks"),
         ("sum(rate(kimistore_uploader_missed_events_total[5m]))", "missed")],
    ))

    p.append(row("Storage and compaction"))

    p.append(ts(
        "Segments merged vs deleted",
        "Consolidation against retention. The object count only falls when merges outpace deletion; when these "
        "flatten together the count is at steady state.",
        [("sum(rate(kimistore_compactor_segments_merged_total[5m]))", "segments merged"),
         ("sum(rate(kimistore_compactor_merged_segments_written_total[5m]))", "segments written"),
         ("sum(rate(kimistore_retention_segments_deleted_total[5m]))", "segments deleted")],
    ))

    p.append(ts(
        "Compaction budget",
        "Passes that hit their time budget against passes overall. A budget-exhausted rate matching the pass rate "
        "means the compactor is throughput-limited; zero while passes climb means it is simply idle. Those two "
        "look identical in the passes/merges panel alone.",
        [("sum(rate(kimistore_compactor_passes_total[5m]))", "passes"),
         ("sum(rate(kimistore_compactor_budget_exhausted_total[5m]))", "budget exhausted")],
    ))

    p.append(ts(
        "Object store IO",
        "Volume against the backing store. This is what the compactor costs, so it is where compaction competing "
        "with ingest becomes visible.",
        [("sum(rate(kimistore_network_io_bytes_total{direction=\"inbound\"}[5m]))", "inbound"),
         ("sum(rate(kimistore_network_io_bytes_total{direction=\"outbound\"}[5m]))", "outbound")],
        unit="Bps",
    ))

    p.append(row("Durability and cluster"))

    p.append(ts(
        "Durable flushes and wait",
        "The flush interval is a durability bound for acks=all, not a size target, so it cannot simply be raised. "
        "Durable wait is also a histogram: mean is _sum over _count, the tail comes from _bucket.",
        [("sum(rate(kimistore_durable_flushes_total[5m]))", "flushes"),
         ("sum(rate(kimistore_offsets_flushed_total[5m]))", "offsets flushed"),
         ("rate(kimistore_durable_wait_seconds_sum[5m]) / clamp_min(rate(kimistore_durable_wait_seconds_count[5m]), 1e-9)", "wait mean"),
         ("histogram_quantile(0.99, sum by (le) (rate(kimistore_durable_wait_seconds_bucket[5m])))", "wait p99")],
    ))

    p.append(ts(
        "Routing",
        "How fresh the routing inventory is, and whether agents disagree about it.",
        [("max(kimistore_routing_age_seconds)", "age"),
         ("sum(rate(kimistore_routing_publish_conflicts_total[5m]))", "publish conflicts"),
         ("sum(rate(kimistore_routing_inventory_failures_total[5m]))", "inventory failures"),
         ("sum(rate(kimistore_routing_inconsistent_tables_total[5m]))", "inconsistent tables")],
    ))

    layout(p)
    assert_no_overlap(p)
    return p


def main():
    out = sys.argv[1] if len(sys.argv) > 1 else "dashboards/kimistore.json"
    panels = build()
    d = {
        "uid": "kimistore",
        "title": "Kimistore",
        "description": (
            "Operational view of the kimistore broker on knode3. "
            "Generated by dashboards/build_dashboard.py -- edit that, not this."
        ),
        "tags": ["kimistore", "mimir", "storage"],
        "editable": True,
        "timezone": "browser",
        "graphTooltip": 1,
        "schemaVersion": 39,
        "version": 1,
        "refresh": "30s",
        "time": {"from": "now-6h", "to": "now"},
        "panels": panels,
        "annotations": {"list": [{
            "builtIn": 1,
            "datasource": {"type": "grafana", "uid": "-- Grafana --"},
            "enable": True,
            "hide": True,
            "iconColor": "rgba(0, 211, 255, 1)",
            "name": "Annotations & Alerts",
            "type": "dashboard",
        }]},
        "links": [],
        "templating": {"list": []},
    }
    json.dump(d, open(out, "w"), indent=2)
    kinds = {}
    for p in panels:
        kinds[p["type"]] = kinds.get(p["type"], 0) + 1
    print(f"  wrote {out}")
    print(f"  panels: {len(panels)}  {kinds}")
    print(f"  queries: {sum(len(p.get('targets', [])) for p in panels)}")
    print("  no overlapping cells")


if __name__ == "__main__":
    main()
