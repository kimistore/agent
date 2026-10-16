#!/usr/bin/env python3
"""Create the kimistore alert rules in Grafana.

Every expression here was verified against Mimir before being used -- all the
kimistore metrics exist (job="kimistore", cluster="homelab"), and the disk
expression was checked against node_exporter on knode3 specifically, because
knode3 did not have an exporter at all until this session.

The `for` durations are the interesting part. The single most important rule
(durable timeouts) has for=5m rather than something short, because this agent is
restarted by hand for deploys and in-flight acks=all produces time out during the
restart -- 43 of them did during the v1.0.26 deploy. A short `for` would page on
every deploy. Even at 5m this fires within minutes of a wedge instead of the
eight hours the real one went unnoticed.
"""
import json
import subprocess
import sys

GF = "https://gf.internal.loafoe.com"
FOLDER = "kimistore"

RULES = [
    dict(
        title="Kimistore: durable acks timing out",
        expr="rate(kimistore_durable_timeouts_total[5m])",
        for_="5m",
        severity="critical",
        no_data="NoData",
        summary="Producers are being acked with records that never reached object storage",
        description=(
            "An acks=all produce has been waiting longer than its deadline for the "
            "record to become durable, so Mimir's distributor is receiving errors "
            "and dropping the samples. On 2026-10-16 this signature went "
            "unnoticed for 8h15m and lost the data permanently. "
            "The durable frontier is most likely wedged; check "
            "kimistore_durable_flushes_total is still climbing and restart the agent."
        ),
    ),
    dict(
        title="Kimistore: retention has stopped reclaiming",
        expr="increase(kimistore_retention_segments_deleted_total[6h])",
        for_="30m",
        severity="warning",
        no_data="NoData",
        cmp="lt",
        threshold=1,
        summary="Retention has deleted nothing for 6 hours, so disk is growing unbounded",
        description=(
            "Retention is the only thing bounding disk, and it is currently the only "
            "policy (7d age, KIMISTORE_RETENTION_BYTES=-1). If it stops reclaiming, "
            "nothing else will. Expect pressure on /home within a day or so."
        ),
    ),
    dict(
        title="Kimistore: WAL recovery truncated records",
        expr="increase(kimistore_wal_recovery_truncations_total[5m])",
        for_="0s",
        severity="critical",
        no_data="NoData",
        summary="The WAL was truncated on recovery -- records were lost at restart",
        description=(
            "A torn or corrupt tail was discarded when the agent started. Data written "
            "but not yet flushed is gone and cannot be recovered. Check the restart "
            "window in the agent log for how many records went."
        ),
    ),
    dict(
        title="Kimistore: recovery found records past the manifest",
        expr="increase(kimistore_objects_reconciled_total[1h])",
        for_="0s",
        severity="warning",
        no_data="NoData",
        summary="Recovery corrected the durable position from object storage",
        description=(
            "A segment was visible before the manifest describing it, which is normal "
            "on a crash, but a non-zero rate means the tail of the system is failing "
            "more often than the dashboards suggest. Per the HA doc this should be "
            "close to zero in steady state."
        ),
    ),
    dict(
        title="Kimistore: offset commits failed to persist",
        expr="rate(kimistore_offset_flush_failures_total[5m])",
        for_="2m",
        severity="warning",
        no_data="NoData",
        summary="A consumer offset commit could not be written to object storage",
        description=(
            "Since 1.0.26 offsets are written through before the client is acked, so "
            "each of these is a commit Grafana told a consumer had succeeded but "
            "which is not durable. Expect a replay-from-start on the next restart."
        ),
    ),
    dict(
        title="Kimistore: object store timing out",
        expr="rate(kimistore_object_store_timeouts_total[5m])",
        for_="5m",
        severity="warning",
        no_data="NoData",
        summary="RustFS is not answering within the agent's timeout",
        description=(
            "Slow object storage stalls acks for the partition being written "
            "(head-of-line), and makes handovers unsafe -- a handover that cannot make "
            "its tail durable keeps the partition rather than losing it."
        ),
    ),
    dict(
        title="Kimistore: agent is not live",
        expr="kimistore_agent_live",
        for_="2m",
        severity="critical",
        no_data="Alerting",
        cmp="lt",
        threshold=1,
        summary="The agent has stopped renewing its liveness record",
        description=(
            "An agent that cannot prove it is alive stops coordinating immediately -- "
            "there is no grace window, because a coordinator has no fencing token. "
            "Consumers will rebalance and producers should be routed elsewhere."
        ),
    ),
    dict(
        title="Knode3: disk usage above 80%",
        expr=(
            '1 - (node_filesystem_avail_bytes{job="node-exporter",mountpoint="/",'
            'instance="knode3.loafoe.net:9100"} / '
            'node_filesystem_size_bytes{job="node-exporter",mountpoint="/",'
            'instance="knode3.loafoe.net:9100"})'
        ),
        for_="15m",
        severity="warning",
        no_data="Alerting",
        cmp="gt",
        threshold=0.8,
        summary="knode3 root filesystem is filling up",
        description=(
            "/ and /home are the same filesystem (/dev/sda2). This is the only disk "
            "threshold left: the byte retention cap was removed in favour of 7d age "
            "only. At ~9 GB/day a 7d window projects to ~63 GB against 917 GB total."
        ),
    ),
]


def alertmanager_config():
    out = subprocess.run(
        ["kubectl", "-n", "monitoring", "get", "secret",
         "grafana-admin-credentials", "-o", "jsonpath={.data.admin-user}"],
        capture_output=True, text=True)
    import base64
    return base64.b64decode(out.stdout).decode()


def password():
    import base64
    out = subprocess.run(
        ["kubectl", "-n", "monitoring", "get", "secret",
         "grafana-admin-credentials", "-o", "jsonpath={.data.admin-password}"],
        capture_output=True, text=True)
    return base64.b64decode(out.stdout).decode()


def build(rule):
    evaluator = {"params": [rule.get("threshold", 0)], "type": rule.get("cmp", "gt")}
    data = [
        {
            "refId": "A",
            "relativeTimeRange": {"from": 600, "to": 0},
            "datasourceUid": "mimir",
            "model": {"expr": rule["expr"], "instant": True, "range": False, "refId": "A"},
        },
        {
            "refId": "B",
            "relativeTimeRange": {"from": 600, "to": 0},
            "datasourceUid": "__expr__",
            "model": {"type": "reduce", "expression": "A", "reducer": "last", "refId": "B"},
        },
        {
            "refId": "C",
            "relativeTimeRange": {"from": 600, "to": 0},
            "datasourceUid": "__expr__",
            "model": {
                "type": "threshold",
                "expression": "B",
                "conditions": [{
                    "evaluator": evaluator,
                    "operator": {"type": "and"},
                    "query": {"params": ["C"]},
                    "reducer": {"params": [], "type": "last"},
                    "type": "query",
                }],
                "refId": "C",
            },
        },
    ]
    return {
        "title": rule["title"],
        "folderUID": FOLDER,
        "ruleGroup": "kimistore",
        "condition": "C",
        "data": data,
        "noDataState": rule["no_data"],
        "execErrState": "Alerting",
        "for": rule["for_"],
        "labels": {"severity": rule["severity"], "service": "kimistore"},
        "annotations": {"summary": rule["summary"], "description": rule["description"]},
    }


def main():
    user, pw = alertmanager_config(), password()

    # Replace any previous run so re-running is idempotent.
    existing = subprocess.run(
        ["curl", "-sk", "-u", f"{user}:{pw}", f"{GF}/api/v1/provisioning/alert-rules"],
        capture_output=True, text=True).stdout
    try:
        for r in json.loads(existing):
            if r.get("folderUID") == FOLDER:
                subprocess.run(
                    ["curl", "-sk", "-u", f"{user}:{pw}", "-X", "DELETE",
                     f"{GF}/api/v1/provisioning/alert-rules/{r['uid']}"],
                    capture_output=True)
                print(f"  removed previous: {r['title']}")
    except Exception:
        pass

    ok = 0
    for rule in RULES:
        body = build(rule)
        res = subprocess.run(
            ["curl", "-sk", "-u", f"{user}:{pw}", "-X", "POST",
             "-H", "Content-Type: application/json",
             "--data", json.dumps(body),
             "-w", "\n%{http_code}",
             f"{GF}/api/v1/provisioning/alert-rules"],
            capture_output=True, text=True).stdout
        lines = res.strip().split("\n")
        code = lines[-1]
        if code == "202":
            print(f"  created  {rule['title']:<48} (for={rule['for_']}, {rule['severity']})")
            ok += 1
        else:
            print(f"  FAILED   {rule['title']}")
            print(f"           {lines[0][:200]}")
    print(f"\n  {ok}/{len(RULES)} rules created")


if __name__ == "__main__":
    main()