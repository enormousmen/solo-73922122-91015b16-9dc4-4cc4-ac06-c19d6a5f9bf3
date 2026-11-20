#!/usr/bin/env bash
# End-to-end demo against a running lease API.
#
#   docker compose up -d --build
#   ./scripts/demo.sh                  # defaults to http://127.0.0.1:8000
#   BASE=http://host:8000 ./scripts/demo.sh
set -euo pipefail

BASE="${BASE:-http://127.0.0.1:8000}"
TOKEN="${OPERATOR_TOKEN:-}"
AUTH=()
if [[ -n "$TOKEN" ]]; then
  AUTH=(-H "X-Operator-Token: $TOKEN")
fi

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
api() {
  local method="$1" path="$2" body="${3:-}"
  if [[ -n "$body" ]]; then
    curl -sS -X "$method" "$BASE$path" -H 'Content-Type: application/json' "${AUTH[@]}" -d "$body"
  else
    curl -sS -X "$method" "$BASE$path" "${AUTH[@]}"
  fi
}
pretty() { python3 -m json.tool; }

say "Health"
api GET /health | pretty

say "Current pool"
api GET /v1/pool | pretty

say "Client laptop-01 acquires a lease"
LAPTOP=$(api POST /v1/leases '{"client_id":"laptop-01"}')
echo "$LAPTOP" | pretty
LAPTOP_ID=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["lease_id"])' <<<"$LAPTOP")

say "Client laptop-01 asks again: same unexpired lease is returned"
api POST /v1/leases '{"client_id":"laptop-01"}' | pretty

say "Client tablet-02 acquires a lease"
TABLET=$(api POST /v1/leases '{"client_id":"tablet-02"}')
echo "$TABLET" | pretty
TABLET_IP=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["ip"])' <<<"$TABLET")

say "laptop-01 renews"
api POST "/v1/leases/$LAPTOP_ID/renew" '{"ttl_seconds":7200}' | pretty

say "Operator imports leases exported from the old DHCP service (one entry already expired)"
IMPORT_BODY=$(python3 - <<'PY'
import json
from datetime import datetime, timedelta, timezone
exp = (datetime.now(timezone.utc) + timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%SZ")
print(json.dumps({
    "batch_id": "legacy-export-1",
    "entries": [
        {"client_id": "legacy-printer", "ip": "10.20.0.200", "expires_at": exp},
        {"client_id": "legacy-scanner", "ip": "10.20.0.201", "expires_at": exp},
        {"client_id": "legacy-retired", "ip": "10.20.0.202", "expires_at": "2001-01-01T00:00:00Z"},
    ],
}))
PY
)
api POST /v1/admin/lease-imports "$IMPORT_BODY" | pretty

say "Imported device re-claims its original address with its legacy id"
api POST /v1/leases '{"client_id":"legacy-printer"}' | pretty

say "Replaying the same batch returns the stored receipt (replayed: true)"
api POST /v1/admin/lease-imports "$IMPORT_BODY" | pretty

say "Same batch id with a different list is rejected"
DIFFERENT=$(python3 - <<'PY'
import json
from datetime import datetime, timedelta, timezone
exp = (datetime.now(timezone.utc) + timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%SZ")
print(json.dumps({
    "batch_id": "legacy-export-1",
    "entries": [{"client_id": "someone-else", "ip": "10.20.0.210", "expires_at": exp}],
}))
PY
)
api POST /v1/admin/lease-imports "$DIFFERENT" | pretty

say "A batch conflicting with a live lease is rejected as a whole (nothing is imported)"
CONFLICT=$(TABLET_IP="$TABLET_IP" python3 - <<'PY'
import json, os
from datetime import datetime, timedelta, timezone
exp = (datetime.now(timezone.utc) + timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%SZ")
print(json.dumps({
    "batch_id": "legacy-export-2",
    "entries": [
        {"client_id": "fine-device", "ip": "10.20.0.220", "expires_at": exp},
        {"client_id": "clashing-device", "ip": os.environ["TABLET_IP"], "expires_at": exp},
    ],
}))
PY
)
api POST /v1/admin/lease-imports "$CONFLICT" | pretty

say "Operator switches the pool (10.20.0.0/24 -> 10.30.0.0/27)"
api PUT /v1/pool '{"cidr":"10.30.0.0/27"}' | pretty

say "Out-of-pool addresses still occupied (with expiry) — imported leases included"
api GET /v1/admin/out-of-pool | pretty

say "The import receipt still replays after the pool switch"
api POST /v1/admin/lease-imports "$IMPORT_BODY" | pretty

say "Renewing laptop-01 (address now outside pool) is rejected"
api POST "/v1/leases/$LAPTOP_ID/renew" '{}' | pretty

say "A new client gets a fresh address from the new pool"
api POST /v1/leases '{"client_id":"phone-03"}' | pretty

say "laptop-01 releases its out-of-pool address -> it is no longer occupied"
api POST "/v1/leases/$LAPTOP_ID/release" | pretty
api GET /v1/admin/out-of-pool | pretty

say "Invalid pool is rejected; the old pool keeps serving"
api PUT /v1/pool '{"cidr":"10.30.0.0/40"}' | pretty
api GET /v1/pool | pretty

say "Done. Data persists in the 'lease-data' volume across restarts; re-posting the import batch replays the stored receipt."
