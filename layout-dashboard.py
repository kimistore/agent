#!/usr/bin/env python3
"""Lay out a dashboard's panels on the grid, and prove the result does not overlap.

Panels are placed left to right in the order they appear, wrapping at 24 columns,
with a new band after each row header. Widths are chosen per panel kind rather
than carried over from the file, because the widths are what a hand-shift gets
wrong: moving panels down without re-flowing the row leaves a wide panel sitting
on top of narrower ones at the same coordinates, and Grafana's frontend does not
recover from that gracefully.

This exists because that is exactly what happened. Three panels ended up at y=5,
one of them 24 wide over two 12 wide ones, and the symptom was a generic
"An unexpected error happened" in the browser with nothing useful in the server
log -- every query returned status=ok. A layout defect with a frontend-only
symptom is invisible to any check that only looks at query results.
"""
import json
import sys

COLUMNS = 24


def width_for(panel):
    """Width and height by panel kind."""
    if panel.get("type") == "row":
        return COLUMNS, 1
    if panel.get("type") == "stat":
        # Four across leaves room for six, so a row of five stats does not wrap
        # and orphan one onto a line of its own.
        return 4, 4
    if panel.get("type") == "timeseries":
        return 12, 7
    return 12, 6


def group_widths(panels):
    """Give each run of consecutive timeseries panels equal widths.

    Without this, two panels in the same group end up different widths purely
    because of how many targets each has, which is not a layout signal. Equal
    widths per group is what makes a row read as a row.
    """
    run = []

    def flush():
        if not run:
            return
        n = len(run)
        # Three per row is the widest that still reads as a panel rather than a
        # strip. Past three, wrap onto a new row instead of going narrower.
        per_row = min(n, 3)
        w = max(6, (COLUMNS // per_row) // 2 * 2)   # even, so n=2 divides 24
        for p in run:
            p["_w"] = w
        run.clear()

    for p in panels:
        if p.get("type") == "timeseries":
            run.append(p)
        else:
            flush()
    flush()


def layout(panels):
    group_widths(panels)
    y = x = 0
    band = 0
    for p in panels:
        w = p.pop("_w", None) or width_for(p)[0]
        h = width_for(p)[1]
        if p.get("type") == "row":
            if x:
                y += band
                x = 0
            p["gridPos"] = {"h": 1, "w": COLUMNS, "x": 0, "y": y}
            y += 1
            band = 0
            continue
        if x + w > COLUMNS:
            y += band
            x = 0
            band = 0
        p["gridPos"] = {"h": h, "w": w, "x": x, "y": y}
        x += w
        band = max(band, h)
    return panels


def overlaps(panels):
    cells = {}
    for p in panels:
        g = p.get("gridPos") or {}
        if "x" not in g:
            continue
        for dy in range(g["h"]):
            for dx in range(g["w"]):
                cells.setdefault((g["y"] + dy, g["x"] + dx), []).append(p.get("title", "?"))
    return {k: v for k, v in cells.items() if len(v) > 1}


def main():
    path = sys.argv[1]
    d = json.load(open(path))
    panels = d.get("panels", [])

    before = overlaps(panels)
    print(f"  panels: {len(panels)}   overlapping cells before: {len(before)}")
    for k, v in sorted(before.items())[:3]:
        print(f"    y={k[0]} x={k[1]}: {' <-> '.join(sorted(set(v)))}")


    layout(panels)

    after = overlaps(panels)
    print(f"  overlapping cells after: {len(after)}")

    print("  --- resulting layout ---")
    for p in panels:
        g = p["gridPos"]
        kind = p.get("type", "?")
        print(f"    y={g['y']:<3} x={g['x']:<3} {g['w']}x{g['h']:<3} {kind:<11} {p.get('title','')}")

    if after:
        print("\n  STILL OVERLAPPING -- not writing")
        sys.exit(1)
    json.dump(d, open(path, "w"), indent=2)
    print("\n  written, no overlaps")


if __name__ == "__main__":
    main()
