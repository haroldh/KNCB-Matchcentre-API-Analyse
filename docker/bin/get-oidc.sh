#!/usr/bin/env bash
set -euo pipefail

# Vereiste IdP-omgevingsvariabelen
: "${KC_TOKEN_URL:?}"
: "${KC_CLIENT_ID:?}"
: "${KC_CLIENT_SECRET:?}"
: "${KC_AUDIENCE:?}"

# Vraag een token op via client_credentials (pas zo nodig aan voor jouw IdP)
resp=$(curl -sS -X POST "$KC_TOKEN_URL" \
  -d grant_type=client_credentials \
  -d client_id="$KC_CLIENT_ID" \
  -d client_secret="$KC_CLIENT_SECRET" \
  -d audience="$KC_AUDIENCE")

# IdP levert vaak een id_token; zo niet, gebruik access_token als het een JWT is met iss/aud/exp/iat
idtok=$(echo "$resp" | jq -r '.id_token // .access_token')
if [ -z "$idtok" ] || [ "$idtok" = "null" ]; then
  echo "Kon geen id/access token vinden in IdP-respons" >&2
  exit 1
fi

# Conservatieve vervaltijd: nu + 3300s (55m)
now=$(date +%s)
exp=$(( now + 3300 ))

# Vereist uitvoerformaat voor executable-sourced creds (OIDC)
# Zie docs: GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES=1 en JSON-structuur. :contentReference[oaicite:3]{index=3}
cat <<JSON
{
  "version": 1,
  "success": true,
  "token_type": "urn:ietf:params:oauth:token-type:id_token",
  "id_token": "$idtok",
  "expiration_time": $exp
}
JSON
