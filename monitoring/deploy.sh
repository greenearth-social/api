#!/usr/bin/env bash
#
# Render and deploy the "Load Test & Bottleneck Attribution" dashboard.
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
# (consumed by the load-test deep-link tooling).

set -euo pipefail

PROJECT_ID="greenearth-471522"

# Cluster-scoped queries (ES exporter, GKE page-cache proxies) always point at the
# prod cluster: stage api reads the prod Elasticsearch, so there is no stage ES to
# chart. Only the api custom-metric namespace differs between environments.
CLUSTER="greenearth-prod-cluster"
K8S_NAMESPACE="greenearth-prod"

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
DISPLAY_NAME="Load Test & Bottleneck Attribution (${ENV_NAME})"

RENDERED="$(mktemp -t bottleneck-dashboard).json"
trap 'rm -f "$RENDERED"' EXIT

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

if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "rendered OK (${ENV_NAME}): ${DISPLAY_NAME}"
    echo "  namespace=${NAMESPACE} cluster=${CLUSTER} k8s_namespace=${K8S_NAMESPACE}"
    exit 0
fi

EXISTING="$(gcloud monitoring dashboards list \
    --project "$PROJECT_ID" \
    --filter "displayName='${DISPLAY_NAME}'" \
    --format 'value(name)' | head -n1)"

if [[ -n "$EXISTING" ]]; then
    echo "updating existing dashboard: ${EXISTING}"
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
