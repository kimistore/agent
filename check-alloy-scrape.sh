#!/bin/sh
# Is Alloy actually scraping the kimistore target?
#
# The ServiceMonitor being "found" and the target being scraped are different
# things, and the second is the one that matters. Alloy exports its own
# component metrics, so this reads those rather than trying to parse its UI
# endpoints: scrape counters settle the question directly.
set -u
NS=k8s-observability-monitoring
POD=$(kubectl get pod -n "$NS" -l app.kubernetes.io/name=custom-alloy -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
echo "     alloy pod: $POD"

echo
echo "  --- alloy's own scrape metrics ---"
kubectl exec -n "$NS" "$POD" -c alloy -- sh -c \
  'curl -s -m 10 http://localhost:12345/metrics 2>/dev/null || wget -q -O- http://localhost:12345/metrics 2>/dev/null' \
  2>/dev/null | grep -iE "scrape|sdk.*target" | grep -v "^#" | head -15 | sed 's/^/     /'

echo
echo "  --- does the otelcol prometheus receiver see targets? ---"
kubectl exec -n "$NS" "$POD" -c alloy -- sh -c \
  'curl -s -m 10 http://localhost:12345/metrics 2>/dev/null' 2>/dev/null \
  | grep -iE "receiver.*prometheus|otelcol_receiver_accepted" | grep -v "^#" | head -8 | sed 's/^/     /'

echo
echo "  --- components health ---"
kubectl exec -n "$NS" "$POD" -c alloy -- sh -c \
  'curl -s -m 10 http://localhost:12345/api/v0/web/components 2>/dev/null' 2>/dev/null \
  | tr ',' '\n' | grep -iE "health|message" | head -12 | sed 's/^/     /'
