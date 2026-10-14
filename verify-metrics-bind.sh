#!/bin/sh
# Verify the metrics bind change did what it was supposed to, and nothing else.
#
# The bind change is the easy half. The half that matters is that the restart did
# not cost ingest: a change to the serving process is exactly the kind of
# operation that has already caused a visible gap on this box once, so the
# before/after is compared on ingest, not just on "is the port listening".
set -u
echo "  === agent ==="
~/kimistore/bin/kimistore -version | sed 's/^/  /'
echo "  metrics bind : $(ss -tln 2>/dev/null | grep ':9101' | awk '{print $4}')"
echo "  kafka bind   : $(ss -tln 2>/dev/null | grep ':9092' | awk '{print $4}')"
echo "  live         : $(curl -s -o /dev/null -w '%{http_code}' -m 5 http://127.0.0.1:9101/live)"

echo
echo "  === ingest (last 5 min) ==="
echo "  uploads      : $(journalctl -u kimistore --since '-5min' --no-pager 2>/dev/null | grep -c 'Uploaded and trimmed')"
echo "  objects      : $(ls -1 /home/andy/RustFS/data/kimistore/mimir-distributor/0 2>/dev/null | wc -l)"
echo "  loss / errors: $(journalctl -u kimistore --since '-8min' --no-pager 2>/dev/null | grep -ciE 'object is gone|panic|level=error')"
echo "  mimir series : $(curl -s --max-time 20 'http://127.0.0.1:9012/prometheus/api/v1/query?query=count(up)' 2>/dev/null | python3 -c 'import json,sys
try:
    print(json.load(sys.stdin)["data"]["result"][0]["value"][1])
except Exception:
    print("?")' 2>/dev/null)"

echo
echo "  === metrics actually served ==="
echo "  kimistore_* samples on loopback: $(curl -s -m 10 http://127.0.0.1:9101/metrics 2>/dev/null | grep -cE '^kimistore_')"
