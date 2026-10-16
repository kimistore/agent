# Alerting

Eight Grafana alert rules covering the kimistore agent on knode3, delivered to
ntfy at `https://ntfy.internal.loafoe.com` (see `deploy/README.md`).

## The rules

| Rule | Expression | `for` | Severity | Fires when |
|---|---|---|---|---|
| Durable acks timing out | `rate(kimistore_durable_timeouts_total[5m])` | 5m | critical | an `acks=all` produce could not become durable in its deadline |
| Retention stopped reclaiming | `increase(kimistore_retention_segments_deleted_total[6h])` < 1 | 30m | warning | retention has deleted nothing for 6h |
| WAL recovery truncated | `increase(kimistore_wal_recovery_truncations_total[5m])` | 0s | critical | a torn WAL tail was discarded at restart |
| Recovery found records past manifest | `increase(kimistore_objects_reconciled_total[1h])` | 0s | warning | recovery corrected the durable position |
| Offset commits failed | `rate(kimistore_offset_flush_failures_total[5m])` | 2m | warning | a commit acked as durable was not written |
| Object store timing out | `rate(kimistore_object_store_timeouts_total[5m])` | 5m | warning | RustFS missed the agent's timeout |
| Agent not live | `kimistore_agent_live` < 1 | 2m | critical | the agent stopped renewing its liveness record |
| knode3 disk > 80% | `1 - avail/size` > 0.8 | 15m | warning | the only disk ceiling left |

### Why the `for` durations are what they are

The durable-timeout rule is the one that matters, and it is **5m rather than
something short on purpose**. This agent is restarted by hand for deploys, and
in-flight `acks=all` produces time out during the restart — 43 of them during
the v1.0.26 deploy alone. A short `for` would page on every deploy. At 5m it
still fires within minutes of a wedge, against the **8h15m** the real one on
2026-10-16 went unnoticed.

The disk rule is 15m because the metric is read once a minute and the value
moves slowly; firing on a single sample would be noise.

### `noDataState` differs deliberately

The disk and agent-live rules use `Alerting`, because their metrics *disappear*
when the thing they watch is gone — node_exporter stops reporting, the agent
stops reporting. Treating NoData as healthy there would mean a dead host looks
fine.

The other six use `NoData`, because their counters legitimately have no series
while nothing is wrong.

## Why only one rule for "the agent is down"

There is no rule on agent liveness as a heartbeat. `kimistore_agent_live` is 1
whenever the agent is running and healthy, so a rule `== 0` catches the fenced
case but not "the process died". A dead process means the scrape fails, and the
right detector for that is a Prometheus `up{job="kimistore"} == 0` rule — which
belongs in the monitoring namespace's own rule set rather than here, so it is
not created by this script.

## Creating them

```bash
scp create-alert-rules.py kmaster:/tmp/ && ssh kmaster 'python3 /tmp/create-alert-rules.py'
```

Idempotent: it deletes anything already in the `kimistore` folder first. It runs
as the Grafana cluster admin, read from `monitoring/grafana-admin-credentials`,
and never prints either credential.

## Verifying a rule actually works

`state=normal` does not distinguish a rule that correctly evaluated to zero from
one stuck on NoData. `prove-alert-fires.sh` forces a real rule to fire by giving
the disk rule a threshold its current value already exceeds, waits, confirms the
message arrives on the topic, then restores the rule.

```bash
scp prove-alert-fires.sh kmaster:/tmp/ && ssh kmaster 'bash /tmp/prove-alert-fires.sh'
```

Last run: fired at 23:41:35, message delivered with `prio=4`, rule restored to
80% / 15m and back to `normal`.

## The data these rules depend on

`kimistore_*` metrics reach Mimir via `job="kimistore"`, `cluster="homelab"`.
The disk rule needs `job="node-exporter"` on `knode3.loafoe.net:9100`, which did
not exist until this session — knode3 had no exporter, so there was no disk
metric for the one host holding the data. See the alloy config on knode3
(`/home/andy/alloy/config.alloy`, job `node-exporter`).