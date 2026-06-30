#!/usr/bin/env bash
# Post a sample QueryCompletedEvent to a running sink's /ingest endpoint.
#
# Usage: scripts/curl-ingest.sh [base-url] [fixture]
#   base-url  defaults to http://localhost:8080
#   fixture   defaults to testdata/query_completed_event.json
#
# The queryId and timestamps are rewritten to "now" so the row appears in the
# dashboard's default last-7-days view and is unique per run.
set -euo pipefail

BASE_URL="${1:-http://localhost:8080}"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURE="${2:-$REPO_DIR/testdata/query_completed_event.json}"

NOW="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
QID="manual_$(date -u +%Y%m%d_%H%M%S)_00000_curl"

PAYLOAD="$(sed \
  -e "s|\"queryId\": \"[^\"]*\"|\"queryId\": \"${QID}\"|" \
  -e "s|\"createTime\": \"[^\"]*\"|\"createTime\": \"${NOW}\"|" \
  -e "s|\"executionStartTime\": \"[^\"]*\"|\"executionStartTime\": \"${NOW}\"|" \
  -e "s|\"endTime\": \"[^\"]*\"|\"endTime\": \"${NOW}\"|" \
  "$FIXTURE")"

echo "POST ${BASE_URL}/ingest  (queryId=${QID})"
curl -sS -o /dev/null -w "HTTP %{http_code}\n" \
  -X POST -H 'Content-Type: application/json' \
  --data-binary "$PAYLOAD" \
  "${BASE_URL}/ingest"
