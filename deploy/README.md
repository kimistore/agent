# ntfy — self-hosted alert channel

Grafana delivers alerts to **https://ntfy.internal.loafoe.com**, a single-replica
ntfy running on the homelab k3s cluster. Not ntfy.sh — it is internal, and it
holds the alert history.

## Layout

| Piece | Where |
|---|---|
| Manifests | `deploy/ntfy.yaml` |
| Namespace | `ntfy` |
| Pod | scheduled onto an rpi (`second` at time of writing) |
| Storage | `nfs-csi` PVC, 1 Gi, named `ntfy-data` |
| Route | HTTPRoute `ntfy/ntfy` → Gateway `default/internal` (192.168.2.131) |
| TLS | the gateway's existing `wildcard-tls-secret`, which already covers `*.internal.loafoe.com` |
| Grafana contact point | `ntfy` (webhook), one contact point, one notification policy |
| Credentials | Grafana has **none** — see "Authentication" below |

Nothing new was created for ingress. The `internal` Cilium Gateway already had
HTTP:80 and HTTPS:443 listeners with `allowedRoutes: All`, so the HTTPRoute was
the only piece required.

## Authentication

`auth-default-access: deny-all`. Access is granted as:

| Principal | Access |
|---|---|
| `grafana` (admin) | read-write, all topics — for reading history |
| anonymous | **write-only on `kimistore-alerts` only** |
| everything else | denied |

So Grafana needs no credential, while the alert history still requires the admin
password and no other topic is reachable.

**The cost:** anything that can reach the ntfy port can append to
`kimistore-alerts` — a spoofed "disk full" is possible from the LAN. This was
chosen over a Grafana-held credential only because Grafana 13.2.3 provides no
API path to a secured field; see below. To close it, set Basic auth on the
contact point in the Grafana UI:

```
user:     grafana
password: (see `kubectl -n ntfy exec deploy/ntfy -- ntfy password reset grafana`)
```

Then revoke the anonymous grant:

```bash
kubectl -n ntfy exec deploy/ntfy -- ntfy access everyone kimistore-alerts deny
```

## Why Grafana holds no credential

Grafana 13.2.3 Open Source cannot be given a secured field through its API:

- `POST/PUT /api/v1/provisioning/contact-points` accepts `secureSettings`,
  returns **202**, and silently drops them — `secureFields` reads back as
  `None`. This was reproduced as the cluster admin, not only as the MCP
  service account.
- `POST /api/alertmanager/grafana/config/api/v1/receivers` is **404** in v13
  (it existed in v9/v10).
- An extra `Authorization` header via `httpHeaderName1`/`httpHeaderValue1` did
  not reach ntfy; Grafana appears to manage that header itself.

ntfy also constrains the credential: `?tk=<token>` on the publish URL is
**rejected with 403** in ntfy 2.31, and only `Authorization: Bearer <token>`
works. So a token in the URL — which the Grafana docs explicitly permit as a
non-secured option — would not have worked either.

A token was minted and scoped (write-only on the one topic) before this was
settled, so `grafana-alerts` exists in ntfy but is unused. Remove with:

```bash
kubectl -n ntfy exec deploy/ntfy -- ntfy user remove grafana-alerts
```

## The contact point body

Grafana's `data` map is **not** used as the request body. The documented
mechanism is `payload`, and the API type is an object, not a string:

```json
"payload": {
  "template": "{\"topic\":\"kimistore-alerts\",\"title\":{{ .CommonLabels.alertname | data.ToJSON }},\"message\":{{ .CommonAnnotations.summary | data.ToJSON }}}",
  "variables": {}
}
```

Three things had to be right, each found from Grafana's own log rather than by
reading docs first:

1. **`data.ToJSON`**, not bare interpolation. A summary containing a quote or
   newline would otherwise produce invalid JSON and a 400.
2. **`.CommonLabels` / `.CommonAnnotations`, not `.Title` / `.Message`.** Those
   two are webhook *settings* fields; in the custom-payload template they fail
   with `can't evaluate field Title in type *templates.ExtendedData`.
3. **`priority` and `tags` are query parameters, not body fields.** ntfy types
   `priority` as a number and `tags` as an array; a JSON body can only carry
   strings, and ntfy rejects the whole message with
   `40024 invalid request: request body must be valid JSON`.

So the URL is:

```
https://ntfy.internal.loafoe.com/?priority=4&tags=rotating_light
```

## Diagnosing this

Grafana's receiver-test endpoint
(`/apis/notifications.alerting.grafana.app/v1beta1/namespaces/default/receivers/{uid}/test`)
reported `unknown integration type` even for a contact point that delivered
correctly, so it is not a reliable signal. The log line to read is:

```bash
kubectl -n monitoring logs -l app.kubernetes.io/name=grafana -c grafana --since 5m \
  | grep "Notify for alerts failed" | sed 's/.*err=//'
```

The error progression that got this working, for anyone repeating it:

| Error | Cause |
|---|---|
| `unknown integration type` | contact point settings malformed |
| `40009 invalid request: topic invalid` | `data` ignored; Grafana sent its default payload, which has no `topic` |
| `template: can't evaluate field Title` | `.Title` is not in the custom-payload context |
| `40024 request body must be valid JSON` | `priority`/`tags` sent as body strings instead of query params |
| `40301 forbidden` | body correct, credential missing |

## Verifying

```bash
GW=$(kubectl get gateway internal -n default -o jsonpath='{.status.addresses[0].value}')

# anonymous: may publish to the alert topic, nothing else
curl -sk -o /dev/null -w '%{http_code}\n' --resolve ntfy.internal.loafoe.com:443:$GW \
  -H 'Content-Type: application/json' \
  -d '{"topic":"kimistore-alerts","message":"anon"}' \
  https://ntfy.internal.loafoe.com/                       # want 200

curl -sk -o /dev/null -w '%{http_code}\n' --resolve ntfy.internal.loafoe.com:443:$GW \
  'https://ntfy.internal.loafoe.com/kimistore-alerts/json?poll=1'   # want 403

curl -sk -o /dev/null -w '%{http_code}\n' --resolve ntfy.internal.loafoe.com:443:$GW \
  -H 'Content-Type: application/json' -d '{"topic":"other","message":"x"}' \
  https://ntfy.internal.loafoe.com/                       # want 403
```

## One replica, deliberately

ntfy's auth database is a single file with no replication, so two writers would
corrupt it. `strategy: Recreate` and `ReadWriteOnce` both exist to guarantee only
one pod ever touches the volume. A second replica would need a real database
behind it, which is not worth it for an alert channel.

No CPU limit is set on purpose: ntfy is IO-bound, and a CFS throttle on a shared
rpi would present as dropped alerts.