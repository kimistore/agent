#!/bin/sh
# Are the remote-write failures still happening, or were they all during the restart?
#
# The distributor errors were timestamped at the moment the agent was restarted,
# which is the expected cause: a broker that is down cannot be written to. But
# "expected" is what one says before checking, and the earlier confusion in this
# project came from a plausible cause that turned out to be the wrong one.
#
# So this separates errors during the restart window from errors after it.
set -u
SINCE=${1:-2026-10-13T00:00:00Z}

echo "  === Mimir2 distributor: errors since $SINCE ==="
docker logs --since "$SINCE" mimir2-distributor 2>&1 \
  | grep -E "failed (pushing )?to partitions|send data to partitions" \
  | grep -oE "^ts=[0-9T:.Z-]+" | cut -c4-25 | sort | uniq -c | tail -12 | sed 's/^/     /'

echo
echo "  === last distributor error ==="
docker logs --since 24h mimir2-distributor 2>&1 \
  | grep -E "send data to partitions" | tail -1 \
  | grep -oE "ts=[^ ]+" | sed 's/^/     /'
echo "  now: $(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo
echo "  === can Mimir2 still reach kimistore? ==="
docker exec mimir2-distributor sh -c 'true' 2>/dev/null || true
echo "     kafka port from the box: $(timeout 5 bash -c 'cat </dev/null >/dev/tcp/127.0.0.1/9092' 2>/dev/null && echo open || echo closed)"

echo
echo "  === is the agent healthy right now? ==="
echo "     live : $(curl -s -o /dev/null -w '%{http_code}' -m 5 http://127.0.0.1:9101/live)"
echo "     uploads/2min: $(journalctl -u kimistore --since '-2min' --no-pager 2>/dev/null | grep -c 'Uploaded and trimmed')"

echo
echo "  === alloy export health, last 10m ==="
NS=k8s-observability-monitoring
POD=$(kubectl get pod -n "$NS" -l app.kubernetes.io/name=custom-alloy -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
kubectl logs -n "$NS" "$POD" -c alloy --since 10m 2>/dev/null \
  | grep -c "Exporting failed" | sed 's/^/     export failures: /'
kubectl logs -n "$NS" "$POD" -c alloy --since 10m 2>/dev/null \
  | grep -oE "found service monitor|Starting scrape|Scrape job|kimistore" | sort | uniq -c | head -5 | sed 's/^/     /'
