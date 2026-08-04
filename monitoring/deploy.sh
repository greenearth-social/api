#!/usr/bin/env bash
#
# Render and deploy the API performance dashboard.
#
#   ./monitoring/deploy.sh stage            # create or update the stage dashboard
#   ./monitoring/deploy.sh prod --dry-run   # render + validate JSON only, no gcloud
#
# Template mechanism is deliberately dependency-free: bottleneck.json.tmpl holds
# literal ${ENV} / ${NAMESPACE} / ${CLUSTER} / ${K8S_NAMESPACE} tokens which are
# substituted here with sed. Cloud Monitoring's own legend syntax
# (${metric.labels.foo}) survives untouched because only those four exact tokens
# are replaced.
#
# On a successful create/update the resulting dashboard resource id is written to
# monitoring/dashboards/ids.env as DASHBOARD_ID_STAGE=... / DASHBOARD_ID_PROD=...
# (consumed by the load-test deep-link in scripts/load_test/analyze.py).

set -euo pipefail

PROJECT_ID="greenearth-471522"

# Cluster-scoped queries (ES exporter, GKE page-cache proxies) target the
# environment's own cluster, matching how the api is wired to Elasticsearch.
# Set below, once the environment argument is parsed.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE="${SCRIPT_DIR}/dashboards/bottleneck.json.tmpl"
IDS_FILE="${SCRIPT_DIR}/dashboards/ids.env"

usage() {
    echo "usage: $(basename "$0") <stage|prod> [--dry-run]" >&2
    exit 2
}

ENV_NAME=""
DRY_RUN=0
for arg in "$@"; do
    case "$arg" in
        stage|prod) ENV_NAME="$arg" ;;
        --dry-run) DRY_RUN=1 ;;
        -h|--help) usage ;;
        *) echo "unknown argument: $arg" >&2; usage ;;
    esac
done
[[ -n "$ENV_NAME" ]] || usage
[[ -f "$TEMPLATE" ]] || { echo "template not found: $TEMPLATE" >&2; exit 1; }

NAMESPACE="$ENV_NAME"
CLUSTER="greenearth-${ENV_NAME}-cluster"
K8S_NAMESPACE="greenearth-${ENV_NAME}"

RENDER_DIR="$(mktemp -d -t bottleneck-dashboard)"
trap 'rm -rf "$RENDER_DIR"' EXIT
RENDERED="${RENDER_DIR}/dashboard.json"

sed \
    -e "s|\${ENV}|${ENV_NAME}|g" \
    -e "s|\${NAMESPACE}|${NAMESPACE}|g" \
    -e "s|\${K8S_NAMESPACE}|${K8S_NAMESPACE}|g" \
    -e "s|\${CLUSTER}|${CLUSTER}|g" \
    "$TEMPLATE" >"$RENDERED"

if grep -qE '\$\{(ENV|NAMESPACE|CLUSTER|K8S_NAMESPACE)\}' "$RENDERED"; then
    echo "ERROR: unsubstituted template tokens remain in the rendered dashboard" >&2
    grep -nE '\$\{(ENV|NAMESPACE|CLUSTER|K8S_NAMESPACE)\}' "$RENDERED" >&2
    exit 1
fi

python3 -m json.tool "$RENDERED" >/dev/null

# The display name is the dashboard's identity for lookup, so take it from the
# rendered template rather than keeping a second copy here that can drift.
DISPLAY_NAME="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["displayName"])' "$RENDERED")"

if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "rendered OK (${ENV_NAME}): ${DISPLAY_NAME}"
    echo "  namespace=${NAMESPACE} cluster=${CLUSTER} k8s_namespace=${K8S_NAMESPACE}"
    exit 0
fi

MATCHES="$(gcloud monitoring dashboards list \
    --project "$PROJECT_ID" \
    --filter "displayName='${DISPLAY_NAME}'" \
    --format 'value(name)')"
MATCH_COUNT="$(printf '%s' "$MATCHES" | grep -c . || true)"

# Renaming the dashboard orphans the old one (the lookup is by display name),
# and two dashboards sharing a name means every later deploy silently updates
# whichever came back first. Refuse rather than guess.
if [[ "$MATCH_COUNT" -gt 1 ]]; then
    echo "ERROR: ${MATCH_COUNT} dashboards already share the name '${DISPLAY_NAME}':" >&2
    printf '  %s\n' $MATCHES >&2
    echo "Delete the stale one(s) with 'gcloud monitoring dashboards delete <name>'." >&2
    exit 1
fi

EXISTING="$(printf '%s' "$MATCHES" | head -n1)"

if [[ -n "$EXISTING" ]]; then
    echo "updating existing dashboard: ${EXISTING}"
    # Cloud Monitoring rejects an update whose config carries no etag
    # ("Update Dashboard should specify a non empty etag") -- it is the
    # optimistic-concurrency token, and it lives on the deployed resource
    # rather than in the template, so read it and splice it in.
    ETAG="$(gcloud monitoring dashboards describe "$EXISTING" \
        --project "$PROJECT_ID" --format 'value(etag)')"
    if [[ -z "$ETAG" ]]; then
        echo "ERROR: could not read the current etag for ${EXISTING}" >&2
        exit 1
    fi
    python3 - "$RENDERED" "$ETAG" <<'ETAG_PY'
import json
import sys

path, etag = sys.argv[1], sys.argv[2]
with open(path) as handle:
    dashboard = json.load(handle)
dashboard["etag"] = etag
with open(path, "w") as handle:
    json.dump(dashboard, handle)
ETAG_PY
    DASHBOARD_NAME="$(gcloud monitoring dashboards update "$EXISTING" \
        --project "$PROJECT_ID" \
        --config-from-file="$RENDERED" \
        --format 'value(name)')"
    # `update` echoes the resource it wrote; fall back to the known name.
    DASHBOARD_NAME="${DASHBOARD_NAME:-$EXISTING}"
else
    echo "creating dashboard: ${DISPLAY_NAME}"
    DASHBOARD_NAME="$(gcloud monitoring dashboards create \
        --project "$PROJECT_ID" \
        --config-from-file="$RENDERED" \
        --format 'value(name)')"
fi

if [[ -z "$DASHBOARD_NAME" ]]; then
    echo "ERROR: could not determine dashboard resource name after deploy" >&2
    exit 1
fi

DASHBOARD_UID="${DASHBOARD_NAME##*/}"
KEY="DASHBOARD_ID_$(echo "$ENV_NAME" | tr '[:lower:]' '[:upper:]')"

mkdir -p "$(dirname "$IDS_FILE")"
touch "$IDS_FILE"
TMP_IDS="$(mktemp)"
grep -v "^${KEY}=" "$IDS_FILE" >"$TMP_IDS" || true
echo "${KEY}=${DASHBOARD_NAME}" >>"$TMP_IDS"
LC_ALL=C sort "$TMP_IDS" | grep -v '^[[:space:]]*$' >"$IDS_FILE"
rm -f "$TMP_IDS"

echo "deployed: ${DASHBOARD_NAME}"
echo "wrote ${KEY} to ${IDS_FILE}"
echo "url: https://console.cloud.google.com/monitoring/dashboards/builder/${DASHBOARD_UID}?project=${PROJECT_ID}"
