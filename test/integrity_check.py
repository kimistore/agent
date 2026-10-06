#!/usr/bin/env python3
"""Integrity checks for the Mimir e2e.

The expectation is *computed*, never captured from the system under test: the
pusher derives each sample value from its (series, point) index, so this can
recompute exactly what should be there. A value read back, written to a file, and
then compared against itself would agree with a bug that corrupts both sides the
same way.
"""

import json
import sys
import urllib.parse
import urllib.request

TIMEOUT = 180


def query(base, tenant, promql, at):
    params = urllib.parse.urlencode({"query": promql, "time": at})
    req = urllib.request.Request(
        f"{base}/prometheus/api/v1/query?{params}",
        headers={"X-Scope-OrgID": tenant},
    )
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            body = json.load(r)
    except urllib.error.HTTPError as e:
        # Mimir rejects a bad query with a body that says why. Swallowing it turns
        # a harness mistake into a silent data failure.
        detail = e.read().decode("utf-8", "replace")[:400]
        raise RuntimeError(f"HTTP {e.code} for: {promql}\n  {detail}") from None
    if body.get("status") != "success":
        raise RuntimeError(f"query failed: {promql} -> {body.get('error')}")
    return body["data"]["result"]


def expect(series, points):
    """(count, sum) the pusher promised for one series."""
    return points, points * series * 100 + points * (points - 1) // 2


def selector_for(lo, hi):
    """A selector matching exactly series [lo, hi).

    Exact alternation rather than a numeric range: a range would quietly pick up
    the other volume's series, and a count that includes them looks fine while
    proving nothing about this group.
    """
    names = "|".join(f"kimi_test_metric_{s}" for s in range(lo, hi))
    return '{__name__=~"%s"}' % names


def keyed(result):
    """Map results by series.

    Keyed on `instance` rather than `__name__`: a range function such as
    count_over_time drops the metric name from the returned labels, so a checker
    that looks for it reports every series as missing no matter what the data is.
    """
    out = {}
    for r in result:
        instance = r.get("metric", {}).get("instance", "")
        if instance.startswith("host-"):
            out[int(instance[5:].split(":")[0])] = r["value"][1]
    return out


def main():
    cfg = json.loads(sys.argv[1])
    base, tenant = cfg["base"], cfg["tenant"]
    lo, hi, points, span, at = (
        cfg["lo"], cfg["hi"], cfg["points"], cfg["span"], cfg["at"],
    )
    sel = selector_for(lo, hi)

    counts = keyed(query(base, tenant, f"count_over_time({sel}[{span}s])", at))
    sums = keyed(query(base, tenant, f"sum_over_time({sel}[{span}s])", at))

    problems = []
    for s in range(lo, hi):
        want_n, want_sum = expect(s, points)
        if s not in counts:
            problems.append(f"series {s}: no count_over_time result")
            continue
        have_n = int(counts[s])
        if have_n != want_n:
            problems.append(f"series {s}: {have_n} samples, expected {want_n}")
        if s not in sums:
            problems.append(f"series {s}: no sum_over_time result")
        elif abs(float(sums[s]) - want_sum) > 0.5:
            problems.append(
                f"series {s}: values sum to {sums[s]}, expected {want_sum}"
            )

    if problems:
        for p in problems[:12]:
            print(f"FAIL {p}")
        if len(problems) > 12:
            print(f"FAIL ... and {len(problems) - 12} more")
        sys.exit(1)

    print(f"     {hi - lo} series x {points} samples: counts and value sums all exact")


if __name__ == "__main__":
    main()