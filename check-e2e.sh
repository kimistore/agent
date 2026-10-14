#!/bin/sh
# End-to-end check that kimistore metrics reach Grafana.
#
# Split by host deliberately. An earlier version of this ran entirely on the
# control plane, so its checks of 127.0.0.1:9101 and /dev/tcp/127.0.0.1/9092 were
# probing the control plane rather than knode3, and it cheerfully reported the
# agent as down while the agent was serving requests a second later. A health
# check that runs on the wrong host is worse than none: it produces a confident
# wrong answer.
#
#   HOST=knode3  (via KIMISTORE)   agent health, port binds, Mimir2 distributor
#   HOST=control (via kubectl)     ServiceMonitor discovery, Alloy export health
set -u

case "${1:-all}" in
knode3)
  echo "  --- agent on knode3 ---"
  echo "     live          : $(curl -s -o /dev/null -w '%{http_code}' -m 5 http://127.0.0.1:9101/live)"
  echo "     metrics bind  : $(ss -tln 2>/dev/null | grep ':9101' | awk '{print $4}')"
  echo "     kafka bind    : $(ss -tln 2>/dev/null | grep ':9092' | awk '{print $4}')"
  echo "     uploads / 2min: $(journalctl -u kimistore --since '-2min' --no-pager 2>/dev/null | grep -c 'Uploaded and trimmed')"
  echo "     loss / errors : $(journalctl -u kimistore --since '-5min' --no-pager 2>/dev/null | grep -ciE 'object is gone|panic|level=error')"
  echo
  echo "  --- Mimir2 distributor: push failures in the last 30m ---"
  N=$(docker logs --since 30m mimir2-distributor 2>&1 | grep -c "send data to partitions" || true)
  echo "     count: $N"
  docker logs --since 30m mimir2-distributor 2>&1 | grep "send data to partitions" | tail -1 \
    | grep -oE "ts=[^ ]+" | sed 's/^/     last: /'
  echo "     now : $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo "  --- Mimir2 has the metrics? ---"
  for M in kimistore_agent_live kimistore_writer_epoch kimistore_compactor_uncommitted_merged_segments; do
    printf '     %-48s ' "$M"
    curl -s --max-time 30 "http://127.0.0.1:9012/prometheus/api/v1/query?query=$M" | python3 -c "
import json,sys
try:
    r=json.load(sys.stdin)['data']['result']
    print(', '.join(f\"{x['metric'].get('instance','?')}={x['value'][1]}\" for x in r) if r else 'NO DATA')
except Exception:
    print('err')
" 2>/dev/null
  done
  ;;

control)
  echo "  --- ServiceMonitor discovery ---"
  kubectl get servicemonitor -n k8s-observability-monitoring kimistore -o name 2>/dev/null | sed 's/^/     /'
  echo
  echo "  --- Alloy: ServiceMonitor found? scrape errors? ---"
  POD=$(kubectl get pod -n k8s-observability-monitoring -l app.kubernetes.io/name=custom-alloy -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  echo "     alloy pod: $POD"
  echo "     found service monitor (ever): $(kubectl logs -n k8s-observability-monitoring "$POD" -c alloy --tail=2000 2>/dev/null | grep -c 'found service monitor.*kimistore')"
  echo "     export failures last 10m   : $(kubectl logs -n k8s-observability-monitoring "$POD" -c alloy --since 10m 2>/dev/null | grep -c 'Exporting failed')"
  echo "     scrape errors last 10m     : $(kubectl logs -n k8s-observability-monitoring "$POD" -c alloy --since 10m 2>/dev/null | grep -ciE 'scrape.*(failed|error)')"
  ;;
esac
