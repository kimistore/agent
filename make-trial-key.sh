#!/bin/sh
# Create a RustFS policy and user scoped to the trial Mimir's own buckets.
#
# The trial currently runs on the same access key as kimistore, which is the
# key that can read everything in this RustFS -- including production Mimir's
# mimir-blocks. That was demonstrated, not assumed: production block metadata
# was read successfully with it while investigating the trial's missing history.
#
# Naming buckets trial-mimir-* is a comment in a YAML file, not a property
# anything enforces. This makes it enforced: a key that resolves trial-mimir-*
# and gets 403 on everything else, so a script pointed at the wrong prefix fails
# loudly instead of quietly operating on production.
#
# Additive: creates a policy and a user, changes nothing existing. The trial is
# only switched over once the new key is proven to work and proven to be
# restricted.
set -e
ENVFILE="${KIMISTORE_ENV:-/home/andy/kimistore/kimistore.env}"
ADMIN=http://127.0.0.1:9001/minio/admin/v3
AK=$(sed -n 's/^AWS_ACCESS_KEY_ID=//p' "$ENVFILE")
SK=$(sed -n 's/^AWS_SECRET_ACCESS_KEY=//p' "$ENVFILE")
[ -n "$AK" ] && [ -n "$SK" ] || { echo "no credentials in $ENVFILE"; exit 1; }

POLICY_NAME=trial-mimir-only
USER_NAME=mimir2-trial

cat > /tmp/trial-policy.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:*"],
      "Resource": [
        "arn:aws:s3:::trial-mimir-blocks",
        "arn:aws:s3:::trial-mimir-blocks/*",
        "arn:aws:s3:::trial-mimir-ruler",
        "arn:aws:s3:::trial-mimir-ruler/*",
        "arn:aws:s3:::trial-mimir-alertmanager",
        "arn:aws:s3:::trial-mimir-alertmanager/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["sts:AssumeRole"],
      "Resource": ["arn:aws:s3:::*"]
    }
  ]
}
EOF

POLICY=$(cat /tmp/trial-policy.json)

echo "  creating policy $POLICY_NAME"
curl -s -m 20 -X POST --aws-sigv4 "aws:amz:us-east-1:s3" -u "$AK:$SK" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"$POLICY_NAME\",\"policy\":$(printf '%s' "$POLICY" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')}" \
  "$ADMIN/add-canned-policy"
echo

echo "  creating user $USER_NAME"
curl -s -m 20 -X POST --aws-sigv4 "aws:amz:us-east-1:s3" -u "$AK:$SK" \
  -H "Content-Type: application/json" \
  -d "{\"accessKey\":\"\",\"secretKey\":\"\",\"groups\":[],\"policies\":[\"$POLICY_NAME\"]}" \
  "$ADMIN/add-user" > /tmp/adduser.json
echo
head -c 300 /tmp/adduser.json
echo

echo "  attaching policy to $USER_NAME"
curl -s -m 20 -X PUT --aws-sigv4 "aws:amz:us-east-1:s3" -u "$AK:$SK" \
  -H "Content-Type: application/json" \
  -d "{\"policies\":[\"$POLICY_NAME\"]}" \
  "$ADMIN/set-user-or-group-policy?userOrGroup=$USER_NAME"
echo
echo "  done -- read /tmp/adduser.json for the generated key, then verify with verify-trial-key.sh"
