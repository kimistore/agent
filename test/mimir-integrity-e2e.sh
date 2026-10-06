#!/usr/bin/env bash
#
# Integrity end-to-end test: a real Grafana Mimir, a real remote-write client, and
# a volume of data whose every sample value is known in advance.
#
# The existing mimir-e2e.sh proves the plumbing works: Mimir produces, kimistore
# stores, the ingester consumes, the query path returns something. It checks that
# series *exist*. It would pass with half the samples dropped, a value from the
# wrong series, or a log that rewound and silently overwrote acknowledged records.
#
# This one pushes volume and then checks the data back against computed truth:
#
#   * every series has exactly the sample count it should
#   * every series has exactly the value sum it should, which pins the individual
#     values as well as their number
#   * the count is re-checked in sub-windows, so loss is localised rather than
#     averaged away
#   * and all of it again after a broker crash, after a broker restart, and after
#     Mimir re-reads the entire log from scratch
#
# The crash phase is the point. An agent killed between an upload and its next
# checkpoint leaves a manifest that says the log ends earlier than it does. If
# recovery trusts it, the next writer appends over records the previous owner had
# already acknowledged and volume 1 loses its tail -- silently, with no error
# anywhere. Pushing volume 2 after the crash and then re-verifying volume 1 is what
# makes that visible.
#
# Usage:  ./mimir-integrity-e2e.sh [--keep]
#         SERIES=100 POINTS=600 INTERVAL_MS=15000 ./mimir-integrity-e2e.sh
#
# Requires: docker, curl, python3, and a built ../agent (or AGENT=/path/to/agent).

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
AGENT="${AGENT:-$REPO/agent}"
MIMIR_IMAGE="${MIMIR_IMAGE:-grafana/mimir:latest}"
KAFKA_PORT=19092
S3_PORT=19000
NET=kimi-mimir-integrity
DATA="${DATA:-/tmp/kimi-mimir-integrity}"
KEEP=0
[[ "${1:-}" == "--keep" ]] && KEEP=1

# Volume. The defaults are a few thousand segments' worth of samples across four
# partitions: enough that the log spans many segments and many flush cycles, which
# is where a stale manifest does its damage.
VOL1_SERIES="${VOL1_SERIES:-60}"
VOL1_POINTS="${VOL1_POINTS:-400}"
# The interval has to stay small on purpose. Mimir's distributor drops samples that
# fall outside validation.create-grace-period (10m by default) from the newest
# sample it has seen for a series, so a request spanning more than about ten
# minutes silently loses its oldest samples -- which looks exactly like broker
# data loss and is not. 400 points at 1s spans 400s, comfortably inside it.
#
# Time span is not what this test is for: sample volume is, and 60 series x 400
# points x 2 volumes is 48k samples spread over many segments and four
# partitions, which is what makes a rewound log end visible.
INTERVAL_MS="${INTERVAL_MS:-1000}"
PARTITIONS="${PARTITIONS:-4}"
# How many equal slices the window is cut into for the locality check.
SLICES="${SLICES:-6}"

DIST_PORT=9098
QUERY_PORT=9099
PUSH_PORT=9098

pass=0; fail=0
ok()   { pass=$((pass+1)); printf '  PASS  %-52s %s\n' "$1" "${2:-}"; }
bad()  { fail=$((fail+1)); printf '  FAIL  %-52s %s\n' "$1" "${2:-}"; }
step() { printf '\n=== %s ===\n' "$1"; }

cleanup() {
  if [[ $KEEP -eq 0 ]]; then
    docker rm -f mimir-integrity-ingester mimir-integrity-distributor mimir-integrity-query >/dev/null 2>&1
    docker network rm "$NET" >/dev/null 2>&1
    pkill -f "kimistore-integrity-s3" 2>/dev/null
    pkill -f "$AGENT" 2>/dev/null
  fi
}
trap cleanup EXIT

require() { command -v "$1" >/dev/null || { echo "missing required tool: $1" >&2; exit 2; }; }
require docker
require curl
require python3
[[ -x "$AGENT" ]] || { echo "agent binary not found at $AGENT (build it, or set AGENT=)" >&2; exit 2; }

total_samples=$(( VOL1_SERIES * VOL1_POINTS ))
push_at=$(date +%s)
# The window both volumes live in, padded so the range selector comfortably
# contains the oldest and newest samples.
window_s=$(( VOL1_POINTS * INTERVAL_MS / 1000 + 600 ))
start=$(( push_at - VOL1_POINTS * INTERVAL_MS / 1000 - 120 ))
end=$(( push_at + 120 ))
VOL2_LO=$VOL1_SERIES
VOL2_HI=$(( VOL1_SERIES * 2 ))

echo "kimistore integrity e2e"
echo "  volume:  $VOL1_SERIES series x $VOL1_POINTS points, then $VOL1_SERIES more after the crash"
echo "  total:   $total_samples samples pushed, expected back"
echo "  window:  $start .. $end ($window_s s), $PARTITIONS partitions"

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

port_free() {
  pkill -f "$AGENT" 2>/dev/null
  for _ in $(seq 1 20); do
    lsof -nP -iTCP:"$KAFKA_PORT" -sTCP:LISTEN >/dev/null 2>&1 || return 0
    sleep 0.5
  done
  echo "port $KAFKA_PORT is still held by another process" >&2
  exit 2
}

start_agent() { # waldir
  port_free
  mkdir -p "$1"
  AWS_ACCESS_KEY_ID=kimistore AWS_SECRET_ACCESS_KEY=kimistore \
  AWS_REGION=us-east-1 S3_ENDPOINT="http://127.0.0.1:$S3_PORT" S3_BUCKET=kimistore \
  KIMISTORE_LISTEN_ADDR="0.0.0.0:$KAFKA_PORT" \
  KIMISTORE_ADVERTISED_HOST=host.docker.internal KIMISTORE_ADVERTISED_PORT="$KAFKA_PORT" \
  KIMISTORE_WAL_DIR="$1" KIMISTORE_METRICS_ADDR=127.0.0.1:9091 \
  KIMISTORE_AUTO_CREATE_PARTITIONS="$PARTITIONS" \
  KIMISTORE_PARTITION_OWNERSHIP=true \
  "$AGENT" >>"$DATA/agent.log" 2>&1 &
  AGENT_PID=$!
  sleep 3
  if ! kill -0 "$AGENT_PID" 2>/dev/null; then
    echo "agent exited immediately; last lines of $DATA/agent.log:" >&2
    tail -5 "$DATA/agent.log" >&2
    exit 2
  fi
  if ! lsof -nP -iTCP:"$KAFKA_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "agent is running but not listening on $KAFKA_PORT" >&2
    tail -5 "$DATA/agent.log" >&2
    exit 2
  fi
}

stop_agent() {
  [[ -n "${AGENT_PID:-}" ]] || return 0
  kill -TERM "$AGENT_PID" 2>/dev/null
  for _ in $(seq 1 40); do kill -0 "$AGENT_PID" 2>/dev/null || break; sleep 0.5; done
  AGENT_PID=""
}

# kill_agent does what a node failure does, rather than what a deploy does: no
# seal, no final checkpoint, no release. This is the phase 4 path.
kill_agent() {
  [[ -n "${AGENT_PID:-}" ]] || return 0
  kill -KILL "$AGENT_PID" 2>/dev/null
  for _ in $(seq 1 20); do kill -0 "$AGENT_PID" 2>/dev/null || break; sleep 0.5; done
  wait "$AGENT_PID" 2>/dev/null
  AGENT_PID=""
}

common=(
  -ingest-storage.enabled
  -ingest-storage.kafka.address="host.docker.internal:$KAFKA_PORT"
  -ingest-storage.kafka.topic=mimir-distributor
  -ingest-storage.kafka.consumer-group=mimir-integrity-ingesters
  -ingest-storage.kafka.auto-create-topic-default-partitions="$PARTITIONS"
  -blocks-storage.backend=filesystem
  -blocks-storage.filesystem.dir=/data/blocks
  -common.storage.backend=filesystem
  -common.storage.filesystem.dir=/data/chunks
  -ingester.ring.instance-id=ingester-0
  -memberlist.nodename=ingester-0-0
  -memberlist.bind-addr=0.0.0.0
  -memberlist.join=mimir-integrity-ingester:7946
)

start_mimir() {
  docker rm -f mimir-integrity-ingester mimir-integrity-distributor mimir-integrity-query >/dev/null 2>&1
  docker network rm "$NET" >/dev/null 2>&1
  if [[ "${1:-}" == "--fresh-blocks" ]]; then
    rm -rf "$DATA/mimir" && mkdir -p "$DATA/mimir/blocks" "$DATA/mimir/chunks"
  fi
  docker network create "$NET" >/dev/null

  run() { local name=$1 target=$2 port=$3; shift 3
    local publish=()
    [[ -n "$port" ]] && publish=(-p "$port":8080)
    docker run -d --name "$name" --network "$NET" \
      --add-host=host.docker.internal:host-gateway \
      -v "$DATA/mimir":/data "${publish[@]}" \
      "$MIMIR_IMAGE" -target="$target" "${common[@]}" "$@" >/dev/null; }

  run mimir-integrity-ingester    ingester                            ""
  run mimir-integrity-distributor distributor                         "$DIST_PORT"
  run mimir-integrity-query       querier,query-scheduler,query-frontend "$QUERY_PORT" \
      -query-frontend.scheduler-address=127.0.0.1:9095

  for _ in $(seq 1 90); do
    curl -sf -o /dev/null "http://127.0.0.1:$DIST_PORT/ready" && \
    curl -sf -o /dev/null "http://127.0.0.1:$QUERY_PORT/ready" && return
    sleep 1
  done
  echo "Mimir did not become ready" >&2
  docker logs mimir-integrity-ingester 2>&1 | tail -20 >&2
  exit 2
}

# The query path has to reach the ingesters. A literal such as vector(0) is
# answered by the frontend itself and would report success while the ring is empty,
# which is how this test would otherwise pass against nothing.
query_works() {
  local deadline=$(( $(date +%s) + ${1:-120} ))
  while [[ $(date +%s) -lt $deadline ]]; do
    r=$(curl -s -m 15 -G "http://127.0.0.1:$QUERY_PORT/prometheus/api/v1/query" \
          --data-urlencode "query=count_over_time(kimi_test_metric_0[$window_s])" \
          --data-urlencode "time=$end" -H "X-Scope-OrgID: demo" 2>/dev/null)
    [[ "$r" == *'"status":"success"'* ]] && return 0
    sleep 2
  done
  echo "Mimir query path never answered; last: ${r:-none}" >&2
  docker logs mimir-integrity-distributor 2>&1 | grep -iE "error|fail" | tail -5 >&2
  exit 2
}

# sel builds a selector naming exactly the series in [lo, hi). Scoping matters:
# an unscoped match includes the other volume's series, so a count that includes
# them looks right while proving nothing about this group.
sel() { # lo hi
  printf '{__name__=~"%s"}' "$(for ((s=$1; s<$2; s++)); do printf 'kimi_test_metric_%d|' "$s"; done | sed 's/|$//')"
}

# samples_at is the sample count over the given selector, tolerant of a query that
# returns nothing yet.
samples_at() { # selector span at
  curl -s -m 120 -G "http://127.0.0.1:$QUERY_PORT/prometheus/api/v1/query" \
    --data-urlencode "query=sum(count_over_time($1[${2}s]))" \
    --data-urlencode "time=$3" -H "X-Scope-OrgID: demo" 2>/dev/null \
    | python3 -c 'import json,sys
try:
    d=json.load(sys.stdin)
    r=d["data"]["result"]
    print(float(r[0]["value"][1]) if r else 0)
except Exception:
    print(0)' 2>/dev/null
}

integrity() { # lo hi points label
  python3 "$HERE/integrity_check.py" "$(cat <<EOF
{"base":"http://127.0.0.1:$QUERY_PORT","tenant":"demo","lo":$1,"hi":$2,
 "points":$3,"span":$window_s,"at":$end}
EOF
)"
}

# verify waits for ingestion to catch up, then checks. Waiting rather than sleeping
# a fixed time is what keeps this from being a flaky pass-or-fail on slow
# machines: it is an exact assertion once the data lands, and only a genuine
# shortfall ends up counted as a failure.
# settle waits until a selector holds at least want samples, or the deadline
# passes. Group checks use this rather than a fixed sleep so that a slow ingest is
# not reported as lost data.
#
# After a full Mimir restart the ingester re-reads the entire log from offset 0,
# so *both* volumes are still arriving when the first group check runs. Waiting per
# group is not enough there: the first group would time out on its own while the
# second is still being ingested. Step 5 therefore settles the union first.
settle() { # selector want deadline
  local deadline=$(( $(date +%s) + ${3:-${SETTLE:-600}} )) got=0
  while [[ $(date +%s) -lt $deadline ]]; do
    got=$(samples_at "$1" "$2" "$end")
    awk -v a="${got:-0}" -v b="$3" 'BEGIN{exit !(a+0 >= b+0)}' && return 0
    sleep 3
  done
  echo "     settle: ${got:-0} samples, wanted more; $1" >&2
  return 1
}

verify() { # lo hi points label
  local want=$(( ($2 - $1) * $3 ))
  local s; s=$(sel "$1" "$2")
  local got=0
  if ! settle "$s" "$window_s" "$want"; then
    bad "$4" "the window never reached $want samples; the count below is a floor, not a result"
  fi
  got=$(samples_at "$s" "$window_s" "$end")
  if integrity "$1" "$2" "$3"; then
    ok "$4" "$(awk -v g="$got" 'BEGIN{printf "%d samples", g}')"
  else
    bad "$4" "integrity check failed"
  fi
}

# Every slice of the window must hold its own share of the samples. A whole-window
# count can pass while a chunk in the middle is gone, which is exactly the shape of
# damage from a crash: the log end rewinds and a contiguous tail disappears.
# Per-slice sample counts, computed rather than divided.
#
# A whole-window count passes even when a contiguous chunk is missing, which is
# exactly the shape of the damage from a rewound log end: the tail goes, and the
# total drops by the same amount either way. Cutting the window up localises it.
# The expected count for a slice is derived from the pusher's arithmetic -- sample
# p sits at push_at - (points-1-p)*interval -- so the boundary effects of a padded
# window are accounted for instead of papered over with a tolerance.
slice_expect() { # from to -> samples per series expected in (from, to]
  python3 -c "
print(sum(1 for p in range($VOL1_POINTS)
          if $1 < $push_at - ($VOL1_POINTS - 1 - p) * $INTERVAL_MS / 1000 <= $2))"
}

slices() { # lo hi points label
  local step=$(( window_s / SLICES )) miss=0 checked=0 worst=""
  local s; s=$(sel "$1" "$2")
  local series=$(( $2 - $1 ))
  for (( i=0; i<SLICES; i++ )); do
    local at=$(( end - window_s + step * (i + 1) ))
    # A range selector is left-open and right-closed.
    local per from to want got
    from=$(( at - step )); to=$at
    per=$(slice_expect "$from" "$to")
    want=$(( per * series ))
    if [[ $want -eq 0 ]]; then checked=$((checked+1)); continue; fi
    got=$(samples_at "$s" "$step" "$to")
    checked=$((checked+1))
    if ! awk -v a="${got:-0}" -v b="$want" 'BEGIN{exit !(a+0 == b+0)}'; then
      miss=$((miss+1))
      worst="$worst [slice $i: ${got:-0}/$want]"
    fi
  done
  if [[ $miss -eq 0 ]]; then
    ok "$1" "$checked slice(s) hold exactly their share of the samples"
  else
    bad "$1" "$miss of $checked slice(s) wrong:$worst"
  fi
}

# --- run -----------------------------------------------------------------
rm -rf "$DATA"; mkdir -p "$DATA/wal1" "$DATA/wal2" "$DATA/wal3"

PUSHER="$DATA/rwpush"
(cd "$HERE/rwpush" && go build -o "$PUSHER" .) || { echo "cannot build the pusher" >&2; exit 2; }
push() { "$PUSHER" -endpoint "http://127.0.0.1:$PUSH_PORT/api/v1/push" -tenant demo "$@"; }

start_s3
start_agent "$DATA/wal1"
start_mimir
wait_settled=1; query_works 150

step "1. push $total_samples samples through Mimir into kimistore"
if out=$(push -series "$VOL1_SERIES" -points "$VOL1_POINTS" -interval-ms "$INTERVAL_MS" \
           -now "${push_at}000" -batch 20 2>&1); then
  ok "distributor accepted $VOL1_SERIES series" "$out"
else
  bad "distributor accepted $VOL1_SERIES series" "$out"
fi
verify 0 "$VOL1_SERIES" "$VOL1_POINTS" "every series has the right count and values"
slices 0 "$VOL1_SERIES" "$VOL1_POINTS" "no slice of the window is short"

step "2. crash the broker: SIGKILL, no seal, no final checkpoint"
segments_before=$(find "$DATA/s3/kimistore" -name '*.log' 2>/dev/null | wc -l | tr -d ' ')
kill_agent
if grep -q "Storage: closed\|graceful" "$DATA/agent.log"; then
  bad "the agent was killed uncleanly" "it logged a clean shutdown"
else
  ok "agent died without a clean shutdown" "$segments_before segment(s) on disk"
fi

start_agent "$DATA/wal2"
query_works 150
if grep -q "recovered log position" "$DATA/agent.log"; then
  ok "log position recovered from object storage" \
     "$(grep -c 'recovered log position' "$DATA/agent.log") partition(s)"
else
  bad "log position recovered from object storage" "nothing in the agent log"
fi

step "3. push another $total_samples samples onto the recovered log"
if out=$(push -series "$VOL1_SERIES" -series-start "$VOL2_LO" -points "$VOL1_POINTS" \
           -interval-ms "$INTERVAL_MS" -now "${push_at}000" -batch 20 2>&1); then
  ok "distributor accepted the second volume" "$out"
else
  bad "distributor accepted the second volume" "$out"
fi

# The failure this catches: a recovered log end below the previous owner's. Volume
# 2 then overwrites volume 1's tail, and volume 1 is short -- with no error
# anywhere, because every individual write succeeded.
verify 0 "$VOL1_SERIES" "$VOL1_POINTS" "volume 1 intact after the crash"
verify "$VOL2_LO" "$VOL2_HI" "$VOL1_POINTS" "volume 2 appended at the right offsets"

reconciled=$(grep -c "but the manifest said" "$DATA/agent.log" || true)
if [[ "${reconciled:-0}" -gt 0 ]]; then
  ok "recovery noticed the stale manifest" \
     "$reconciled partition(s) reconciled upwards; kimistore_objects_reconciled_total"
else
  ok "no stale manifest to reconcile" "the killed agent happened to have checkpointed"
fi

step "4. restart the broker cleanly with an EMPTY local WAL"
stop_agent
start_agent "$DATA/wal3"
query_works 150
verify 0 "$VOL1_SERIES" "$VOL1_POINTS" "both volumes intact after a broker restart"

step "5. restart Mimir: it re-reads the whole log from kimistore"
start_mimir --fresh-blocks
query_works 180
# Both volumes are still arriving: Mimir starts from offset 0 and has to re-ingest
# everything. Settle the union before asserting either half, or the first check
# reports the second half's samples as the first's missing ones.
settle "$(sel 0 $(( VOL1_SERIES * 2 )))" "$window_s" $(( total_samples * 2 )) 900 || true
verify 0 "$VOL1_SERIES" "$VOL1_POINTS" "volume 1 survives a full Mimir restart"
verify "$VOL2_LO" "$VOL2_HI" "$VOL1_POINTS" "volume 2 survives a full Mimir restart"

step "6. the log start points at data that is really there"
# A high log start is the quiet failure this whole test is for: the records are in
# object storage, nothing errors, and a consumer that resets to the start of the
# log never reads them. Compare ListOffsets("earliest") against the oldest record
# that can actually be fetched.
if command -v docker >/dev/null; then
  probe_out=$(docker run --rm --network "$NET" \
    -v /tmp/kimi-logprobe:/logprobe:ro alpine:3 \
    /logprobe host.docker.internal:"$KAFKA_PORT" mimir-distributor 2>&1)
  earliest=$(echo "$probe_out" | sed -n 's/.*reported earliest offset is \([0-9]*\).*/\1/p')
  first_rec=$(echo "$probe_out" | sed -n 's/.*first=\([0-9]*\).*/\1/p')
  if [[ -n "$first_rec" && -n "$earliest" && "$earliest" -gt "$first_rec" ]]; then
    bad "ListOffsets(earliest) does not hide any record" \
        "earliest=$earliest but records start at $first_rec: $probe_out"
  elif [[ "$probe_out" == *"below the earliest offset are NOT readable"* ]]; then
    bad "records below the reported log start are still readable" "$(echo "$probe_out" | tail -1)"
  else
    ok "ListOffsets(earliest) points at real data" \
       "earliest=$(echo "$probe_out" | sed -n 's/.*partition 0: \(first=[0-9]*\) high-watermark=\([0-9]*\).*/\1 \2/p')"
  fi
fi

step "7. what actually landed in object storage"
segments=$(find "$DATA/s3/kimistore" -name '*.log' 2>/dev/null | wc -l | tr -d ' ')
bytes=$(find "$DATA/s3/kimistore" -name '*.log' -exec cat {} + 2>/dev/null | wc -c | tr -d ' ')
if [[ "${segments:-0}" -gt 0 ]]; then
  ok "segments in object storage" "$segments segment(s), ${bytes:-0} bytes"
else
  bad "segments in object storage" "none found"
fi
want_bytes=$(( total_samples * 2 * 4 ))
if [[ "${bytes:-0}" -ge "$want_bytes" ]]; then
  ok "the log holds a real volume of data" \
     "${bytes} bytes for $((total_samples * 2)) samples (>= $want_bytes)"
else
  bad "the log holds a real volume of data" "only ${bytes:-0} bytes, expected >= $want_bytes"
fi

step "result"
echo "  $pass passed, $fail failed"
[[ $fail -eq 0 ]] || exit 1