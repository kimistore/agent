#!/bin/sh
# Did Alloy actually pick the new ServiceMonitor up?
#
# Applying the objects only proves the API accepted them. Whether Alloy's
# ServiceMonitor discovery found it, and whether the target scraped cleanly, is
# a separate question and the one that matters -- a ServiceMonitor that is
# created but not discovered looks identical to a working one until you query for
# the metrics it was supposed to produce.
set -u
NS=k8s-observability-monitoring

echo "  1. ServiceMonitor exists"
kubectl get servicemonitor -n "$NS" kimistore -o wide 2>/dev/null | sed 's/^/     /'

echo
echo "  2. does Alloy know about the target?"
POD=$(kubectl get pod -n "$NS" -l app.kubernetes.io/name=custom-alloy -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
echo "     alloy pod: $POD"
if [ -n "$POD" ]; then
  kubectl exec -n "$NS" "$POD" -c alloy -- sh -c \
    'wget -q -O- "http://localhost:12345/api/v1/targets?state=active" 2>/dev/null || curl -s "http://localhost:12345/api/v1/targets?state=active" 2>/dev/null' \
    2>/dev/null | tr ',' '\n' | grep -iE "kimistore|9101" | head -10 | sed 's/^/     /'
fi

echo
echo "  3. alloy config reload / errors"
kubectl logs -n "$NS" "$POD" -c alloy --tail=200 2>/dev/null \
  | grep -iE "servicemonitor|kimistore|error" | tail -8 | cut -c1-190 | sed 's/^/     /'

echo
echo "  4. scrape it the way alloy will, as a sanity check"
kubectl run ks --rm -i --restart=Never --image=curlimages/curl:latest --quiet -- \
  curl -s -m 10 http://192.168.2.236:9101/metrics 2>/dev/null \
  | grep -E "^kimistore_(compactor_uncommitted|agent_live|writer_epoch)" | sed 's/^/     /'
