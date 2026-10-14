#!/bin/sh
# Create a RustFS user bound to trial-mimir-only, and prove the key is restricted.
#
# mc is required, and must be pointed at RustFS with root credentials:
#   MC_HOST_trial="http://$RUSTFS_ACCESS_KEY:$RUSTFS_SECRET_KEY@127.0.0.1:9000"
#
# The admin API accepts add-canned-policy as a plain PUT with the name in the
# query string, but add-user rejects a JSON body and wants an encrypted payload
# that only madmin-go can produce. Hence mc, not curl.
#
# The policy is created separately by make-trial-key.sh. This script refuses to
# proceed if it is missing, because creating a user with no policy would produce
# an unrestricted key -- the exact thing this work exists to remove.
set -e
ENVFILE="${KIMISTORE_ENV:-/home/andy/kimistore/kimistore.env}"
MC=${MC:-/tmp/mc}
ALIAS=${MC_ALIAS:-trial}
USER_KEY_PREFIX=${USER_KEY_PREFIX:-MIMIR2TRIAL}
OUT=${1:-/tmp/trial-key.env}
VERIFY=${2:-/tmp/verify-trial-key.sh}

[ -x "$MC" ] || { echo "mc not found at $MC; set MC="; exit 1; }
: "${MC_HOST_trial:?set MC_HOST_trial to root credentials, e.g. MC_HOST_trial=http://KEY:SECRET@127.0.0.1:9000}"

$MC admin policy info "$ALIAS" trial-mimir-only >/dev/null 2>&1 || {
  echo "policy trial-mimir-only does not exist -- run make-trial-key.sh first."
  echo "Creating a user with no policy would hand out an unrestricted key."
  exit 1
}

GEN() { LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c "$1"; }
NEW_AK="$USER_KEY_PREFIX$(GEN 11)"
NEW_SK="$(GEN 40)"

echo "  creating user $NEW_AK"
$MC admin user add "$ALIAS" "$NEW_AK" "$NEW_SK"
$MC admin policy attach "$ALIAS" trial-mimir-only --user "$NEW_AK"

umask 077
printf 'S3_ACCESS_KEY_ID=%s\nS3_SECRET_ACCESS_KEY=%s\n' "$NEW_AK" "$NEW_SK" > "$OUT"
echo "  key written to $OUT (mode 600)"
echo

if [ -x "$VERIFY" ]; then
  # Not optional. A key that works but is not restricted is a second
  # unrestricted credential, not an improvement.
  sh "$VERIFY" "$OUT"
else
  echo "  verify-trial-key.sh not found at $VERIFY -- run it before using this key"
fi
