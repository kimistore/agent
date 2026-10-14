#!/bin/sh
# Preconditions for scraping kimistore's metrics, checked before anything changes.
#
# Two things have to be true or the work is wasted:
#   1. a pod can route to knode3 at all
#   2. Alloy's ServiceMonitor discovery would actually pick up a new one
#
# (1) is proven without touching the agent: Alloy already scrapes
# knode3.loafoe.net:9141, so pods demonstrably reach knode3 on a non-loopback
# port. What is not yet proven is that 9101 is reachable, because the agent binds
# it to 127.0.0.1 -- which is the thing we are about to change.
set -u
NS=${NS:-default}

run() {
  kubectl run "$1" --rm -i --restart=Never --image=curlimages/curl:latest --quiet -- sh -c "$2" 2>&1 | grep -v "Defaulted container"
}

echo "  1. what does knode3.loafoe.net resolve to, from a pod?"
run p1 'nslookup knode3.loafoe.net 2>/dev/null | tail -4'

echo "  2. is 9141 reachable from a pod? (Alloy scrapes it today)"
run p2 'curl -s -o /dev/null -w "     9141 -> %{http_code}\n" -m 6 http://knode3.loafoe.net:9141/metrics'

echo "  3. is 9101 reachable from a pod? (expected: no, it is loopback-only today)"
run p3 'curl -s -o /dev/null -w "     9101 -> %{http_code}\n" -m 6 http://knode3.loafoe.net:9101/metrics'

echo "  4. does Alloy's ServiceMonitor discovery restrict namespaces?"
kubectl get cm k8s-observability-monitoring-custom-alloy -n k8s-observability-monitoring \
  -o jsonpath='{.data.config\.alloy}' 2>/dev/null \
  | awk '/prometheus.operator.servicemonitors/,/^}/' | sed 's/^/     /'
