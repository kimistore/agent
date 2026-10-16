# ntfy — self-hosted alert channel

Grafana delivers alerts to **https://ntfy.internal.loafoe.com**, a single-replica
ntfy running on the homelab k3s cluster. Not ntfy.sh — it is internal, and it
holds the alert history.

## Layout

| Piece | Where |
|---|---|
| Manifests | `deploy/ntfy.yaml` |
| Namespace | `ntfy` (newly created) |
| Pod | scheduled onto an rpi (`second` at time of writing) |
| Storage | `nfs-csi` PVC, 1 Gi, named `ntfy-data` |
| Route | HTTPRoute `ntfy/ntfy` → Gateway `default/internal` (192.168.2.131) |
| TLS | the gateway's existing `wildcard-tls-secret`, which already covers `*.internal.loafoe.com` |
| User | `grafana`, role admin |

Nothing new was created for ingress. The `internal` Cilium Gateway already had
HTTP:80 and HTTPS:443 listeners with `allowedRoutes: All`, so the HTTPRoute was
the only piece required.

## Auth

`auth-default-access: deny-all`, so a topic exists only for a granted user. This
is what stops anything on the LAN from publishing a fake "disk full" onto a
channel you are trying to trust.

The password lives **only** on ntfy and in the Grafana contact point. It is not
in this repo, not in the ConfigMap, and not in any manifest here.

To rotate it:

```bash
kubectl -n ntfy exec deploy/ntfy -- ntfy password reset grafana
```

then update the Grafana contact point to match.

## Grafana contact point

Named `ntfy`, type webhook. Two settings are load-bearing and were both wrong in
earlier attempts:

**1. `data`, not `payload`.** ntfy 2.31 rejects a form-encoded body outright:

```
{"code":40024,"http":400,"error":"invalid request: request body must be valid JSON"}
```

Grafana's `payload` map is form-encoded; `data` is what produces a JSON body.

**2. `priority` and `tags` must not be body fields.** ntfy types `priority` as a
number and `tags` as an array, but Grafana's `data` map is `map[string]string` —
so they arrive as `"4"` and `"rotating_light"` and the *entire* message is
rejected. They are passed as query parameters instead:

```
https://ntfy.internal.loafoe.com/?priority=4&tags=rotating_light
```

Body carries only `topic`, `title`, `message`.

## Verifying

Anonymous publish must be refused, authenticated publish must succeed:

```bash
GW=$(kubectl get gateway internal -n default -o jsonpath='{.status.addresses[0].value}')
curl -sk -o /dev/null -w '%{http_code}\n' --resolve ntfy.internal.loafoe.com:443:$GW \
  -d '{"topic":"kimistore-alerts","message":"anon"}' \
  https://ntfy.internal.loafoe.com/                       # want 401/403

curl -sk -u grafana:PASSWORD --resolve ntfy.internal.loafoe.com:443:$GW \
  -H 'Content-Type: application/json' \
  -d '{"topic":"kimistore-alerts","message":"check"}' \
  https://ntfy.internal.loafoe.com/                       # want 200
```

## One replica, deliberately

ntfy's auth database is a single file with no replication, so two writers would
corrupt it. `strategy: Recreate` and `ReadWriteOnce` both exist to guarantee only
one pod ever touches the volume. A second replica would need a real database
behind it, which is not worth it for an alert channel.

No CPU limit is set on purpose: ntfy is IO-bound, and a CFS throttle on a shared
rpi would present as dropped alerts.