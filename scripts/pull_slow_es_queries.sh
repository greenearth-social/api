#!/usr/bin/env bash
# Pull slow_es_query log entries from Cloud Run for the given environment and time window.
# Outputs one JSON object per line to stdout (NDJSON), suitable for piping to
# scripts/profile_es_queries.py.
#
# Usage:
#   ./scripts/pull_slow_es_queries.sh [--environment stage|prod] [--hours N] [--limit N]
#
# Requires: gcloud CLI authenticated with application-default credentials
#   gcloud auth application-default login
#
# Example:
#   ./scripts/pull_slow_es_queries.sh --environment prod --hours 504 | \
#       pipenv run python scripts/profile_es_queries.py --dry-run

set -euo pipefail

PROJECT_ID="greenearth-471522"
ENVIRONMENT="stage"
HOURS="504"   # 3 weeks = 504 hours
LIMIT="200"

usage() {
    echo "Usage: $0 [--environment stage|prod] [--hours N] [--limit N]" >&2
    exit 1
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --environment) ENVIRONMENT="$2"; shift 2 ;;
        --hours)       HOURS="$2";       shift 2 ;;
        --limit)       LIMIT="$2";       shift 2 ;;
        -h|--help)     usage ;;
        *) echo "Unknown arg: $1" >&2; usage ;;
    esac
done

case "$ENVIRONMENT" in
    stage) SERVICE="greenearth-api-stage" ;;
    prod)  SERVICE="greenearth-api-prod"  ;;
    *)     echo "Unknown environment: $ENVIRONMENT (use stage or prod)" >&2; exit 1 ;;
esac

FILTER="resource.type=\"cloud_run_revision\"
resource.labels.service_name=\"${SERVICE}\"
resource.labels.location=\"us-east1\"
textPayload=~\"slow_es_query\""

gcloud logging read \
    --project="${PROJECT_ID}" \
    --format="json" \
    --freshness="${HOURS}h" \
    --limit="${LIMIT}" \
    "${FILTER}" \
    | python3 -c "
import json, sys

entries = json.load(sys.stdin)
for entry in entries:
    payload = entry.get('textPayload', '') or entry.get('jsonPayload', {}).get('message', '')
    if 'slow_es_query' not in payload:
        continue
    out = {
        'timestamp': entry.get('timestamp', ''),
        'payload': payload,
    }
    print(json.dumps(out))
" 2>/dev/null
