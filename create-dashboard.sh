#!/bin/sh
# Create the kimistore dashboard in Grafana via the API.
#
# The dashboard body is passed as a file rather than inlined: it is 17KB, and
# inlining it through several layers of shell quoting is how it gets mangled.
set -eu
GRAFANA=${GRAFANA:-https://gf.internal.loafoe.com}
DASH=${1:?usage: create-dashboard.sh <dashboard.json> [folder-uid]}
FOLDER=${2:-kimistore}
TOKFILE=${TOKFILE:-/tmp/gftok}

[ -r "$TOKFILE" ] || { echo "no token at $TOKFILE"; exit 1; }
TOKEN=$(cat "$TOKFILE")

BODY=$(python3 - "$DASH" "${2:-}" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
d.pop("id", None)
d.pop("url", None)
d.pop("version", None)
d["id"] = None
payload = {"dashboard": d, "overwrite": True}
# An empty or "none" folder argument means unfiled. Sending a folderUid that
# does not exist is a 400, which is what happens when the folder was just
# deleted but the argument is still on the command line.
if sys.argv[2] and sys.argv[2].lower() != "none":
    payload["folderUid"] = sys.argv[2]
print(json.dumps(payload))
PY
)

RESP=$(mktemp)
CODE=$(curl -s -o "$RESP" -w '%{http_code}' -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  --data-binary "$BODY" \
  "$GRAFANA/api/dashboards/db")

echo "  HTTP $CODE"
python3 -c "
import json,sys
d=json.load(open('$RESP'))
print('   status :', d.get('status'))
if d.get('status') != 'success':
    print('   error  :', str(d.get('message'))[:200]); sys.exit(1)
print('   folder :', d.get('meta',{}).get('folderTitle'), '/', d.get('meta',{}).get('folderUid'))
print('   url    :', d.get('url'))
print('   panels :', len(d.get('dashboard',{}).get('panels',[])))
"
rm -f "$RESP"
