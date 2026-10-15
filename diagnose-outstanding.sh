#!/bin/sh
# Where is "too many outstanding requests" coming from?
#
# It can be three different limits, and they are fixed in three different places:
# Grafana's per-datasource concurrency, the Mimir query scheduler's
# max_outstanding_requests_per_tenant, and the querier's own concurrency. So the
# message alone does not identify the cause -- this checks each in turn rather
# than assuming the first one that comes to mind.
set -u
NS=monitoring

echo "  === grafana: the error and its status codes ==="
POD=$(kubectl get pod -n $NS -l app.kubernetes.io/name=grafana -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
echo "  grafana pod: $POD"
kubectl logs -n $NS "$POD" --since 20m 2>/dev/null \
  | grep -iE "too many outstanding|outstanding request" | tail -4 | cut -c1-200 | sed 's/^/  /'
echo "  non-ok queryData responses, last 20m:"
kubectl logs -n $NS "$POD" --since 20m 2>/dev/null \
  | grep -E "Plugin Request Completed" | grep -vE "status=ok" | tail -6 | cut -c1-200 | sed 's/^/  /'

echo
echo "  === mimir2: which limit is hit ==="
for PORT in 9012 9010 9011 9014; do
  echo "  --- :$PORT ---"
  curl -s -m 15 "http://127.0.0.1:$PORT/metrics" 2>/dev/null \
    | grep -iE "outstanding|max_concurrent|too_many" | grep -v "^#" | head -5 | sed 's/^/    /'
done

echo
echo "  === how long do these queries actually take? ==="
for Q in 'max(kimistore_agent_live)' \
         'sum(rate(kimistore_request_count_total[5m]))' \
         'histogram_quantile(0.99, sum by (le) (rate(kimistore_request_time_milliseconds_total_bucket[5m])))' \
         'sum(rate(kimistore_compactor_segments_merged_total[5m]))'; do
  T=$( { /usr/bin/time -f %e curl -s -o /dev/null -m 30 "http://127.0.0.1:9012/prometheus/api/v1/query?query=$Q"; } 2>&1 )
  echo "    ${T}s  $Q"
done
