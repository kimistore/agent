#!/usr/bin/env bash
# Confirms that kimistore.eu resolves to the GitHub Pages IP addresses and
# that the site answers over HTTPS. Run this after adding the DNS records.
#
# Expected DNS configuration at the provider:
#
#   A     @     185.199.108.153
#   A     @     185.199.109.153
#   A     @     185.199.110.153
#   A     @     185.199.111.153
#   AAAA  @     2606:50c0:8000::153
#   AAAA  @     2606:50c0:8001::153
#   AAAA  @     2606:50c0:8002::153
#   AAAA  @     2606:50c0:8003::153
#   CNAME www   kimistore.github.io
#
# Exits 0 when every check passes, 1 otherwise.

set -uo pipefail

DOMAIN="${1:-kimistore.eu}"
WWW="${2:-www.kimistore.eu}"

# GitHub Pages addresses, from GitHub's own documentation.
want_a=(185.199.108.153 185.199.109.153 185.199.110.153 185.199.111.153)
want_aaaa=(2606:50c0:8000::153 2606:50c0:8001::153 2606:50c0:8002::153 2606:50c0:8003::153)

fail=0

note() { printf '  %-38s %s\n' "$1" "$2"; }
ok()   { printf '  \033[32m%-38s\033[0m %s\n' "$1" "$2"; }
bad()  { printf '  \033[31m%-38s\033[0m %s\n' "$1" "$2"; fail=1; }

resolver() {
  # Prefer a resolver that answers, so a local cache does not produce a
  # false failure while propagation is still in progress.
  for r in 1.1.1.1 8.8.8.8 9.9.9.9; do
    dig +short "$@" "@$r" 2>/dev/null && return 0
  done
  dig +short "$@" 2>/dev/null
}

echo "Records for $DOMAIN"

got_a=($(resolver "$DOMAIN" A -t A))
got_aaaa=($(resolver "$DOMAIN" AAAA -t AAAA))

for ip in "${want_a[@]}"; do
  if printf '%s\n' "${got_a[@]:-}" | grep -qx "$ip"; then
    ok "A $ip" "present"
  else
    bad "A $ip" "MISSING"
  fi
done

for ip in "${want_aaaa[@]}"; do
  if printf '%s\n' "${got_aaaa[@]:-}" | grep -qx "$ip"; then
    ok "AAAA $ip" "present"
  else
    bad "AAAA $ip" "MISSING"
  fi
done

# Extra addresses are not automatically wrong, but they send traffic to
# whatever else claims the name, so they are worth surfacing.
for ip in "${got_a[@]:-}"; do
  [[ -z "$ip" ]] && continue
  if printf '%s\n' "${want_a[@]:-}" | grep -qx "$ip"; then
    continue
  fi
  note "A $ip" "unexpected extra address"
done

cname=$(resolver "$WWW" CNAME -t CNAME | head -1)
if [[ "$cname" == kimistore.github.io* ]]; then
  ok "CNAME $WWW" "$cname"
elif [[ -z "$cname" ]]; then
  note "CNAME $WWW" "not set (optional)"
else
  bad "CNAME $WWW" "$cname"
fi

echo
echo "HTTP"

if [[ "$fail" -eq 1 ]]; then
  note "$DOMAIN" "DNS incomplete, skipping HTTP checks"
  echo
  echo "Result: FAIL"
  exit 1
fi

code=$(curl -s -o /dev/null -w '%{http_code}' -m 20 "https://$DOMAIN/" 2>/dev/null)
case "$code" in
  200) ok "https://$DOMAIN/" "HTTP 200" ;;
  000) bad "https://$DOMAIN/" "no response (certificate pending?)" ;;
  *)   bad "https://$DOMAIN/" "HTTP $code" ;;
esac

if [[ -n "$cname" ]]; then
  target=$(curl -s -o /dev/null -w '%{redirect_url}' -m 20 "https://$WWW/" 2>/dev/null)
  if [[ -z "$target" ]]; then
    ok "https://$WWW/" "serves the site"
  elif [[ "$target" == *"//$DOMAIN"* ]]; then
    ok "https://$WWW/" "redirects to $DOMAIN"
  else
    bad "https://$WWW/" "redirects to $target"
  fi
fi

# The Pages certificate is issued by Let's Encrypt after the domain resolves.
# Until then the hostname presents the wrong certificate.
cert=$(echo | openssl s_client -servername "$DOMAIN" -connect "$DOMAIN:443" 2>/dev/null \
        | openssl x509 -noout -subject -ext subjectAltName 2>/dev/null)
if printf '%s' "$cert" | grep -q "DNS:$DOMAIN"; then
  ok "certificate" "valid for $DOMAIN"
else
  note "certificate" "not yet issued for $DOMAIN"
fi

echo
if [[ "$fail" -eq 0 ]]; then
  echo "Result: PASS"
else
  echo "Result: FAIL"
fi
exit "$fail"
