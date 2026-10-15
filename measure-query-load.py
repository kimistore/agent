#!/usr/bin/env python3
"""How much query load does the dashboard actually put on Mimir, and how slow is it?

429 "too many outstanding requests" is Mimir's queue refusing work, so the useful
numbers are how many requests arrive together and how long one takes. If requests
are fast the queue cannot back up and the cause is the burst size; if they are
slow it backs up at any burst size, and raising the limit only postpones it.

Query latency is measured for a range query over the dashboard's own default
window, because that is what the panels actually issue. An instant query is not
the same shape of work and would understate it.
"""
import json
import time
import urllib.parse
import urllib.request

QUERIER = "http://127.0.0.1:9012/prometheus/api/v1"

RANGE_QUERIES = [
    ("agent live (instant-shaped)", "max(kimistore_agent_live)", True),
    ("request rate", "sum(rate(kimistore_request_count_total[5m]))", False),
    ("latency p99 (histogram)",
     "histogram_quantile(0.99, sum by (le) (rate(kimistore_request_time_milliseconds_total_bucket[5m])))", False),
    ("error rates (11 series)",
     "sum(rate(kimistore_offset_flush_failures_total[5m]))", False),
    ("object store IO", 'sum(rate(kimistore_network_io_bytes_total{direction="inbound"}[5m]))', False),
    ("durable wait p99",
     "histogram_quantile(0.99, sum by (le) (rate(kimistore_durable_wait_seconds_bucket[5m])))", False),
]


def timed(label, query, instant, window_h=6, step="60s"):
    now = int(time.time())
    start = now - window_h * 3600
    if instant:
        url = f"{QUERIER}/query?" + urllib.parse.urlencode({"query": query, "time": now})
    else:
        url = f"{QUERIER}/query_range?" + urllib.parse.urlencode(
            {"query": query, "start": start, "end": now, "step": step})
    t0 = time.time()
    try:
        with urllib.request.urlopen(url, timeout=60) as r:
            body = json.loads(r.read())
        dt = time.time() - t0
        if body.get("status") == "error":
            print(f"  {dt:6.2f}s  {label:32} ERROR {body.get('error')[:60]}")
            return dt, False
        res = body.get("data", {}).get("result", [])
        if not instant:
            pts = sum(len(s.get("values", [])) for s in res)
            print(f"  {dt:6.2f}s  {label:32} {len(res)} series, {pts} points")
        else:
            print(f"  {dt:6.2f}s  {label:32} {len(res)} series")
        return dt, True
    except Exception as exc:
        dt = time.time() - t0
        print(f"  {dt:6.2f}s  {label:32} EXC {str(exc)[:60]}")
        return dt, False


def burst(n, query, window_h=6):
    """Fire n identical range queries at once -- what a dashboard load looks like."""
    now = int(time.time())
    start = now - window_h * 3600
    url = f"{QUERIER}/query_range?" + urllib.parse.urlencode(
        {"query": query, "start": start, "end": now, "step": "60s"})
    t0 = time.time()
    ok = err = 0
    for _ in range(n):
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                body = json.loads(r.read())
            if body.get("status") == "error":
                err += 1
            else:
                ok += 1
        except Exception:
            err += 1
    dt = time.time() - t0
    print(f"  burst of {n:>3} identical range queries: {dt:6.2f}s wall, ok={ok} err={err}")
    return ok, err


def main():
    print("  === single queries, 6h window at 60s step (the dashboard's own shape) ===")
    times = []
    for label, q, instant in RANGE_QUERIES:
        dt, _ = timed(label, q, instant)
        times.append(dt)
    print(f"  slowest single query: {max(times):.2f}s")

    print()
    print("  === concurrent load ===")
    q = "sum(rate(kimistore_request_count_total[5m]))"
    for n in (10, 25, 42, 60):
        burst(n, q)


if __name__ == "__main__":
    main()
