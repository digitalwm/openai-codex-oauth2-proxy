#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ ! -f "$SCRIPT_DIR/.env" ]]; then
  echo "Missing $SCRIPT_DIR/.env" >&2
  exit 1
fi

set -a
source "$SCRIPT_DIR/.env"
set +a

if [[ -z "${PROXY_BEARER_TOKEN:-}" ]]; then
  echo "PROXY_BEARER_TOKEN is not set in $SCRIPT_DIR/.env" >&2
  exit 1
fi

curl -sS http://10.0.0.254:28080/status \
  -H "Authorization: Bearer ${PROXY_BEARER_TOKEN}" \
  | jq '.accounts[] | {
      name,
      healthy,
      primary_remaining: .quota.primary_remaining_percent,
      secondary_remaining: .quota.secondary_remaining_percent,
      primary_reset_s: .quota.primary_reset_after_seconds,
      secondary_reset_s: .quota.secondary_reset_after_seconds,
      plan: .quota.plan_type,
      last_error
    }'
