#!/bin/sh
# Watch the trial compactor catch up, and watch what it costs.
#
# The compactor is doing a first pass over roughly a week of blocks that nothing
# had compacted, and it is doing that I/O against the same RustFS disk kimistore
# writes to. So the thing to watch is not only "is it finishing" but "what is it
# costing the ingest path while it does".
#
# A compactor that is working and a compactor that has stopped look different in
# the merge-job rate, and a compactor that has stalled behind a growing queue is
# different again -- which is the failure this whole session has repeatedly run
# into, where a stopped component and a finished one are indistinguishable.
set -u
B=/home/andy/RustFS/data/trial-mimir-blocks/anonymous
K=/home/andy/RustFS/data/kimistore/mimir-distributor/0
INTERVAL=${1:-60}
ROUNDS=${2:-20}
LOG=${3:-/tmp/watch.log}

printf '%-6s %-8s %-8s %-7s %-7s %-7s %-9s %-8s\n' \
  t blocks jobs blocks/s size load iowait kim-obj up/min > "$LOG"

prev_jobs=0
prev_t=$(date +%s)
round=0
while [ "$round" -lt "$ROUNDS" ]; do
  round=$((round + 1))
  sleep "$INTERVAL"
  now=$(date +%s)
  dt=$((now - prev_t))
  [ "$dt" -lt 1 ] && dt=1

  jobs=$(docker logs --since "${dt}s" mimir2-compactor 2>&1 | grep -cE "job_type=(merge|vertical|delete)" || true)
  rate=$(( (jobs - prev_jobs) * 60 / dt ))
  prev_jobs=$((prev_jobs + (jobs - prev_jobs) / 1))   # cumulative counter
  prev_t=$now

  blocks=$(ls -1 "$B" 2>/dev/null | wc -l | tr -d ' ')
  size=$(du -sh /home/andy/RustFS/data/trial-mimir-blocks 2>/dev/null | cut -f1 | tr -d ' ')
  load=$(cut -d' ' -f1 /proc/loadavg)
  iow=$(vmstat 1 2 2>/dev/null | tail -1 | awk '{print $16}')
  kobj=$(ls -1 "$K" 2>/dev/null | wc -l | tr -d ' ')
  up=$(journalctl -u kimistore --since "-${dt}s" --no-pager 2>/dev/null | grep -c "Uploaded and trimmed" || true)

  printf '%-6s %-8s %-8s %-7s %-7s %-7s %-7s %-9s %-8s\n' \
    "${round}m" "$blocks" "$jobs" "$rate" "$size" "$load" "${iow:-?}" "$kobj" "$up" >> "$LOG"
done
echo "done" >> "$LOG"
