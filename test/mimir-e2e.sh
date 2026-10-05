#!/usr/bin/env bash
#
# End-to-end test: a real Grafana Mimir deployment with kimistore as its Kafka
# landing zone.
#
# What this proves, which unit tests cannot:
#   * Mimir's distributor produces into kimistore over the Kafka protocol
#   * Mimir's ingester consumes from it as a consumer group and writes TSDBs
#   * the data is queryable back through Mimir's query path
#   * the data survives kimistore restarting with an EMPTY local WAL
#
# Mimir 3.x uses twmb/franz-go, not segmentio/kafka-go, so this is the only
# way to check against the client that actually matters.
#
# Usage:  ./mimir-e2e.sh [--keep]
#
# Requires: docker, a built ../agent (or AGENT=/path/to/agent), and an
# S3-compatible endpoint. By default a small local S3 shim is started, so no
# external service is needed.

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
AGENT="${AGENT:-$REPO/agent}"
MIMIR_IMAGE="${MIMIR_IMAGE:-grafana/mimir:latest}"
KAFKA_PORT=19092
S3_PORT=19000
NET=kimi-mimir-e2e
DATA="${DATA:-/tmp/kimi-mimir-e2e}"
KEEP=0
[[ "${1:-}" == "--keep" ]] && KEEP=1

DIST_PORT=9098
QUERY_PORT=9099
PUSH_PORT=9098

pass=0; fail=0
ok()   { pass=$((pass+1)); printf '  PASS  %-54s %s\n' "$1" "${2:-}"; }
bad()  { fail=$((fail+1)); printf '  FAIL  %-54s %s\n' "$1" "${2:-}"; }
step() { printf '\n=== %s ===\n' "$1"; }

cleanup() {
  if [[ $KEEP -eq 0 ]]; then
    docker rm -f mimir-e2e-ingester mimir-e2e-distributor mimir-e2e-query >/dev/null 2>&1
    docker network rm "$NET" >/dev/null 2>&1
    pkill -f "kimistore-e2e-s3" 2>/dev/null
    pkill -f "$AGENT" 2>/dev/null
  fi
}
trap cleanup EXIT

require() { command -v "$1" >/dev/null || { echo "missing required tool: $1" >&2; exit 2; }; }
require docker
require curl
[[ -x "$AGENT" ]] || { echo "agent binary not found at $AGENT (build it, or set AGENT=)" >&2; exit 2; }

# --- local S3 shim -------------------------------------------------------
# A minimal S3-compatible endpoint on the filesystem, so the agent can run
# unmodified without an external object store.
S3_PY="$HERE/s3shim.py"
start_s3() {
  python3 "$S3_PY" "$S3_PORT" "$DATA/s3" >"$DATA/s3.log" 2>&1 &
  for _ in $(seq 1 20); do
    curl -sf -o /dev/null "http://127.0.0.1:$S3_PORT/kimistore?list-type=2&prefix=" && return
    sleep 0.5
  done
  echo "S3 shim did not start; see $DATA/s3.log" >&2
  exit 2
}

# --- kimistore ------------------------------------------------------------
start_agent() { # waldir
  AWS_ACCESS_KEY_ID=kimistore AWS_SECRET_ACCESS_KEY=kimistore \
  AWS_REGION=us-east-1 S3_ENDPOINT="http://127.0.0.1:$S3_PORT" S3_BUCKET=kimistore \
  KIMISTORE_LISTEN_ADDR="0.0.0.0:$KAFKA_PORT" \
  KIMISTORE_ADVERTISED_HOST=host.docker.internal KIMISTORE_ADVERTISED_PORT="$KAFKA_PORT" \
  KIMISTORE_WAL_DIR="$1" KIMISTORE_METRICS_ADDR=9091 \
  "$AGENT" >>"$DATA/agent.log" 2>&1 &
  AGENT_PID=$!
  sleep 3
}

stop_agent() {
  [[ -n "${AGENT_PID:-}" ]] || return 0
  kill -TERM "$AGENT_PID" 2>/dev/null
  for _ in $(seq 1 30); do kill -0 "$AGENT_PID" 2>/dev/null || break; sleep 0.5; done
  AGENT_PID=""
}

# --- Mimir ---------------------------------------------------------------
common=(
  -ingest-storage.enabled
  -ingest-storage.kafka.address="host.docker.internal:$KAFKA_PORT"
  -ingest-storage.kafka.topic=mimir-distributor
  -ingest-storage.kafka.consumer-group=mimir-ingesters
  -blocks-storage.backend=filesystem
  -blocks-storage.filesystem.dir=/data/blocks
  -common.storage.backend=filesystem
  -common.storage.filesystem.dir=/data/chunks
  -ingester.ring.instance-id=ingester-0
  -memberlist.nodename=ingester-0-0
  -memberlist.bind-addr=0.0.0.0
  -memberlist.join=mimir-e2e-ingester:7946
)

start_mimir() {
  docker rm -f mimir-e2e-ingester mimir-e2e-distributor mimir-e2e-query >/dev/null 2>&1
  docker network rm "$NET" >/dev/null 2>&1
  rm -rf "$DATA/mimir" && mkdir -p "$DATA/mimir/blocks" "$DATA/mimir/chunks"
  docker network create "$NET" >/dev/null

  run() { local name=$1 target=$2 port=$3; shift 3
    local publish=()
    [[ -n "$port" ]] && publish=(-p "$port":8080)
    docker run -d --name "$name" --network "$NET" \
      --add-host=host.docker.internal:host-gateway \
      -v "$DATA/mimir":/data "${publish[@]}" \
      "$MIMIR_IMAGE" -target="$target" "${common[@]}" "$@" >/dev/null; }

  # The ingester exposes no HTTP port worth publishing: it is reached through
  # the distributor and the querier over the internal network.
  run mimir-e2e-ingester    ingester                            ""
  run mimir-e2e-distributor distributor                         "$DIST_PORT"
  run mimir-e2e-query       querier,query-scheduler,query-frontend "$QUERY_PORT" \
      -query-frontend.scheduler-address=127.0.0.1:9095

  for _ in $(seq 1 60); do
    curl -sf -o /dev/null "http://127.0.0.1:$DIST_PORT/ready" && \
    curl -sf -o /dev/null "http://127.0.0.1:$QUERY_PORT/ready" && return
    sleep 1
  done
  echo "Mimir did not become ready" >&2
  docker logs mimir-e2e-ingester 2>&1 | tail -20 >&2
  exit 2
}

# /ready goes green before the ingester ring has elected an owner for the
# ingest partition, and a push in that window is rejected with "no active
# partition found". Wait for the query path to answer a real query instead.
wait_settled() { # seconds
  local deadline=$(( $(date +%s) + ${1:-90} ))
  while [[ $(date +%s) -lt $deadline ]]; do
    local r
    # The query has to reach the ingesters: a literal such as vector(0) is
    # answered locally and would report success while the ring is still empty.
    r=$(curl -s -m 10 -G "http://127.0.0.1:$QUERY_PORT/prometheus/api/v1/query" \
          --data-urlencode "query=count(kimi_test_metric_0)" \
          --data-urlencode "time=$(date +%s)" -H "X-Scope-OrgID: demo" 2>/dev/null)
    if [[ "$r" == *'"status":"success"'* ]]; then return 0; fi
    sleep 2
  done
  echo "Mimir did not settle; last response: ${r:-none}" >&2
  return 1
}

# --- helpers -------------------------------------------------------------
push() { "$PUSHER" -endpoint "http://127.0.0.1:$PUSH_PORT/api/v1/push" -tenant demo "$@"; }
q_range() { # query start end step
  curl -s -m 30 -G "http://127.0.0.1:$QUERY_PORT/prometheus/api/v1/query_range" \
    --data-urlencode "query=$1" --data-urlencode "start=$2" --data-urlencode "end=$3" \
    --data-urlencode "step=$4" -H "X-Scope-OrgID: demo"
}
q_series() {
  curl -s -m 30 "http://127.0.0.1:$QUERY_PORT/prometheus/api/v1/label/__name__/values" \
    -H "X-Scope-OrgID: demo"
}

# --- run -----------------------------------------------------------------
rm -rf "$DATA"; mkdir -p "$DATA/wal1" "$DATA/wal2"

# Built after the data directory is created, and into a separate path so a
# restart of the test does not depend on build ordering.
PUSHER="$DATA/rwpush"
(cd "$HERE/rwpush" && go build -o "$PUSHER" .) || { echo "cannot build the remote-write pusher" >&2; exit 2; }

start_s3
start_agent "$DATA/wal1"
start_mimir
wait_settled 120

step "1. Mimir pushes metrics through kimistore"
if out=$(push -series 5 -points 20 2>&1); then
  ok "distributor accepted the write" "$(echo "$out" | head -1)"
else
  bad "distributor accepted the write" "$out"
fi
sleep 10

produced=$(grep -c "ApiKey=0 Version=3" "$DATA/agent.log" || true)
if [[ "${produced:-0}" -gt 0 ]]; then
  ok "Mimir produced with Produce v3" "$produced request(s)"
else
  bad "Mimir produced with Produce v3" "$(grep -oE 'ApiKey=0 Version=[0-9]+' "$DATA/agent.log" | tail -1)"
fi

parse_errors=$(docker logs mimir-e2e-ingester 2>&1 | grep -c "failed to parse write request" || true)
if [[ "${parse_errors:-0}" -eq 0 ]]; then
  ok "ingester parsed every consumed record" "record headers survived the round trip"
else
  bad "ingester parsed every consumed record" "$parse_errors parse error(s)"
fi

step "2. the data is queryable back through Mimir"
names=$(q_series)
for i in 0 1 2 3 4; do
  if [[ "$names" == *"kimi_test_metric_$i"* ]]; then
    ok "series kimi_test_metric_$i is queryable" ""
  else
    bad "series kimi_test_metric_$i is queryable" "$names"
  fi
done

NOW=$(date +%s); START=$(( NOW - 7200 )); END=$(( NOW + 120 ))
res=$(q_range 'kimi_test_metric_0' "$START" "$END" 120)
if [[ "$res" == *'"status":"success"'* && "$res" == *'"values"'* ]]; then
  ok "samples queryable with values" "$(echo "$res" | grep -oE '\[\[[0-9]+,"[0-9]+"' | head -1)"
else
  bad "samples queryable with values" "$(echo "$res" | head -c 200)"
fi

step "3. restart kimistore with an EMPTY local WAL"
stop_agent
segments=$(find "$DATA/s3/kimistore" -name '*.log' 2>/dev/null | wc -l | tr -d ' ')
if [[ "${segments:-0}" -gt 0 ]]; then
  ok "shutdown flushed the log to object storage" "$segments segment(s)"
else
  bad "shutdown flushed the log to object storage" "none found"
fi

start_agent "$DATA/wal2"
wait_settled 60
if grep -q "recovered log position" "$DATA/agent.log"; then
  ok "log position recovered from object storage" "$(grep -c 'recovered log position' "$DATA/agent.log") partition(s)"
else
  bad "log position recovered from object storage" ""
fi

step "4. restart Mimir entirely; the data must still be there"
start_mimir
wait_settled 120
res=$(q_range 'kimi_test_metric_0' "$START" "$END" 120)
if [[ "$res" == *'"status":"success"'* && "$res" == *'"values"'* ]]; then
  ok "data survived a broker restart and a Mimir restart" "$(echo "$res" | grep -oE '\[\[[0-9]+,"[0-9]+"' | head -1)"
else
  bad "data survived a broker restart and a Mimir restart" "$(echo "$res" | head -c 300)"
fi
names=$(q_series)
missing=0
for i in 0 1 2 3 4; do [[ "$names" == *"kimi_test_metric_$i"* ]] || missing=$((missing+1)); done
if [[ $missing -eq 0 ]]; then
  ok "all five series survived" ""
else
  bad "all five series survived" "$missing missing"
fi

step "result"
echo "  $pass passed, $fail failed"
[[ $fail -eq 0 ]] || exit 1
