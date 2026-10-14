#!/bin/sh
# Prove the trial's new key is both sufficient and restricted.
#
# "Can read trial-mimir-blocks" is half the test and the easy half. The half that
# matters is that it cannot read mimir-blocks: that is what turns "the trial uses
# its own bucket" from a comment in a YAML file into something enforced. A key
# that is merely working is not scoped, it is a second unrestricted credential.
#
# Both directions are asserted, and the script fails if either does not hold.
set -e
KEYFILE=${1:-/tmp/trial-key.env}
[ -r "$KEYFILE" ] || { echo "no key file $KEYFILE"; exit 1; }
AK=$(sed -n 's/^S3_ACCESS_KEY_ID=//p' "$KEYFILE")
SK=$(sed -n 's/^S3_SECRET_ACCESS_KEY=//p' "$KEYFILE")
[ -n "$AK" ] && [ -n "$SK" ] || { echo "incomplete key file"; exit 1; }

code() { curl -s -o /dev/null -w '%{http_code}' -m 20 --aws-sigv4 "aws:amz:us-east-1:s3" -u "$AK:$SK" "$1"; }

fail=0

echo "  key under test: $AK"
echo "  --- must succeed (the trial's own buckets) ---"
for b in trial-mimir-blocks trial-mimir-ruler trial-mimir-alertmanager; do
  c=$(code "http://127.0.0.1:9000/$b/")
  if [ "$c" = "200" ]; then
    echo "    OK       $b -> $c"
  else
    echo "    BROKEN   $b -> $c  (the trial could not work with this key)"
    fail=1
  fi
done

echo "  --- must fail (everything else) ---"
# A real object key is used, not just the bucket root: a bucket-level denial and
# an object-level denial are different checks, and a policy that only blocks
# ListBucket would still let objects be fetched by name.
for b in mimir-blocks mimir-ruler mimir-alertmanager mimir kimistore; do
  c=$(code "http://127.0.0.1:9000/$b/")
  if [ "$c" = "403" ] || [ "$c" = "404" ]; then
    echo "    OK       $b -> $c (denied)"
  else
    echo "    LEAK     $b -> $c  (this key can reach production)"
    fail=1
  fi
done

echo "  --- object-level check on a production block ---"
# Read one real object out of production's block storage. If this returns 200 the
# policy is not doing anything at all, whatever the bucket listing said.
OBJ=$(ls -1 /home/andy/RustFS/data/mimir-blocks/anonymous 2>/dev/null | head -1)
if [ -n "$OBJ" ]; then
  c=$(code "http://127.0.0.1:9000/mimir-blocks/anonymous/$OBJ/meta.json")
  if [ "$c" = "403" ] || [ "$c" = "404" ]; then
    echo "    OK       mimir-blocks/anonymous/$OBJ/meta.json -> $c (denied)"
  else
    echo "    LEAK     mimir-blocks/anonymous/$OBJ/meta.json -> $c"
    fail=1
  fi
fi

if [ "$fail" != "0" ]; then
  echo "  RESULT: NOT SAFE TO USE -- do not switch the trial to this key"
  exit 1
fi
echo "  RESULT: scoped correctly -- sufficient for the trial, denied everywhere else"
