#!/usr/bin/env bash

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-greenearth-471522}"
REGION="${REGION:-us-east1}"
RAW_ENVIRONMENT="${E2E_ENVIRONMENT:-${ENVIRONMENT:-stage}}"
if [[ "$RAW_ENVIRONMENT" == "stage" || "$RAW_ENVIRONMENT" == "prod" ]]; then
  ENVIRONMENT="$RAW_ENVIRONMENT"
else
  ENVIRONMENT="stage"
fi
MODEL="${MODEL:-two_tower}"
NUM_CANDIDATES="${NUM_CANDIDATES:-5}"
MAX_USERS_TO_TRY="${MAX_USERS_TO_TRY:-25}"
CANDIDATE_GENERATOR="${CANDIDATE_GENERATOR:-post_similarity}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
  echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
  echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
  echo -e "${RED}[ERROR]${NC} $1"
}

usage() {
  cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --project-id ID        GCP project ID (default: $PROJECT_ID)
  --region REGION        GCP region (default: $REGION)
  --environment ENV      Environment: stage|prod (default: $ENVIRONMENT)
  --api-url URL          API base URL (default: discovered from Cloud Run)
  --model NAME           Ranking model (default: $MODEL)
  --num-candidates N     Candidates to request (default: $NUM_CANDIDATES)
  --max-users N          Max user_dids to try from likes agg (default: $MAX_USERS_TO_TRY)
  --generator NAME       Candidate generator to use (default: $CANDIDATE_GENERATOR)
  --help                 Show this help

Environment variable overrides:
  PROJECT_ID REGION E2E_ENVIRONMENT (or ENVIRONMENT) API_URL MODEL NUM_CANDIDATES MAX_USERS_TO_TRY CANDIDATE_GENERATOR

What this script does:
  1) Discovers an API URL (if not provided)
  2) Fetches API auth key from Secret Manager
  3) Connects to stage/prod GKE and reads Elasticsearch password
  4) Finds users with likes from Elasticsearch
  5) Finds a user that yields non-empty candidates
  6) Calls /rank/predict with that user + candidates
  7) Fails if ranking output is empty
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    log_error "Missing required command: $1"
    exit 1
  fi
}

API_URL="${API_URL:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-id)
      PROJECT_ID="$2"
      shift 2
      ;;
    --region)
      REGION="$2"
      shift 2
      ;;
    --environment)
      ENVIRONMENT="$2"
      shift 2
      ;;
    --api-url)
      API_URL="$2"
      shift 2
      ;;
    --model)
      MODEL="$2"
      shift 2
      ;;
    --num-candidates)
      NUM_CANDIDATES="$2"
      shift 2
      ;;
    --max-users)
      MAX_USERS_TO_TRY="$2"
      shift 2
      ;;
    --generator)
      CANDIDATE_GENERATOR="$2"
      shift 2
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      log_error "Unknown option: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ "$ENVIRONMENT" != "stage" && "$ENVIRONMENT" != "prod" ]]; then
  log_error "ENVIRONMENT must be stage or prod"
  exit 1
fi

require_cmd gcloud
require_cmd kubectl
require_cmd curl
require_cmd jq

if [[ -z "$API_URL" ]]; then
  SERVICE_NAME="greenearth-api-$ENVIRONMENT"
  API_URL=$(gcloud run services describe "$SERVICE_NAME" \
    --platform=managed \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --format='value(status.url)')
  if [[ -z "$API_URL" ]]; then
    log_error "Could not discover API URL for service $SERVICE_NAME"
    exit 1
  fi
fi

if [[ "$ENVIRONMENT" == "prod" ]]; then
  API_KEY_SECRET="api-key-prod"
else
  API_KEY_SECRET="api-key"
fi

log_info "Using API URL: $API_URL"
log_info "Using project: $PROJECT_ID, region: $REGION, environment: $ENVIRONMENT"

API_KEY=$(gcloud secrets versions access latest --secret="$API_KEY_SECRET" --project="$PROJECT_ID")
if [[ -z "$API_KEY" ]]; then
  log_error "Failed to fetch API key from secret $API_KEY_SECRET"
  exit 1
fi

CLUSTER_NAME="greenearth-${ENVIRONMENT}-cluster"
NAMESPACE="greenearth-${ENVIRONMENT}"

log_info "Configuring kubectl context for $CLUSTER_NAME"
gcloud container clusters get-credentials "$CLUSTER_NAME" \
  --location="$REGION" \
  --project="$PROJECT_ID" >/dev/null

ELASTIC_PASSWORD=$(kubectl get secret greenearth-es-elastic-user -n "$NAMESPACE" -o go-template='{{.data.elastic | base64decode}}')
if [[ -z "$ELASTIC_PASSWORD" ]]; then
  log_error "Failed to fetch elastic password from greenearth-es-elastic-user in $NAMESPACE"
  exit 1
fi

PORT_FORWARD_LOG=$(mktemp)
cleanup() {
  if [[ -n "${PF_PID:-}" ]] && kill -0 "$PF_PID" >/dev/null 2>&1; then
    kill "$PF_PID" >/dev/null 2>&1 || true
    wait "$PF_PID" >/dev/null 2>&1 || true
  fi
  rm -f "$PORT_FORWARD_LOG"
}
trap cleanup EXIT

log_info "Starting temporary port-forward to Elasticsearch"
kubectl port-forward service/greenearth-es-http 9200:9200 -n "$NAMESPACE" >"$PORT_FORWARD_LOG" 2>&1 &
PF_PID=$!

for _ in $(seq 1 20); do
  if curl -sk --max-time 2 https://localhost:9200 >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! curl -sk --max-time 2 https://localhost:9200 >/dev/null 2>&1; then
  log_error "Elasticsearch port-forward did not become ready"
  cat "$PORT_FORWARD_LOG"
  exit 1
fi

log_info "Fetching top users with likes from Elasticsearch"
TOP_USERS_JSON=$(curl -sk -u "elastic:$ELASTIC_PASSWORD" \
  'https://localhost:9200/likes/_search' \
  -H 'Content-Type: application/json' \
  -d "{\"size\":0,\"aggs\":{\"top_users\":{\"terms\":{\"field\":\"author_did\",\"size\":$MAX_USERS_TO_TRY,\"order\":{\"_count\":\"desc\"}}}}}")

USER_DIDS=$(echo "$TOP_USERS_JSON" | jq -r '.aggregations.top_users.buckets[].key')
if [[ -z "$USER_DIDS" ]]; then
  log_error "No user_dids found in likes index"
  exit 1
fi

FOUND_USER=""
FOUND_CANDIDATES=""

for USER_DID in $USER_DIDS; do
  log_info "Trying user_did: $USER_DID"

  GEN_PAYLOAD=$(jq -n \
    --arg gen "$CANDIDATE_GENERATOR" \
    --arg did "$USER_DID" \
    --argjson n "$NUM_CANDIDATES" \
    '{generators:[{name:$gen,weight:1}],user_did:$did,num_candidates:$n}')

  GEN_RESPONSE=$(curl -sS \
    -H "X-API-Key: $API_KEY" \
    -H 'Content-Type: application/json' \
    "$API_URL/candidates/generate" \
    -d "$GEN_PAYLOAD")

  CAND_COUNT=$(echo "$GEN_RESPONSE" | jq '.candidates | length' 2>/dev/null || echo 0)
  if [[ "$CAND_COUNT" -gt 0 ]]; then
    FOUND_USER="$USER_DID"
    FOUND_CANDIDATES="$GEN_RESPONSE"
    log_info "Found usable user with $CAND_COUNT candidates: $FOUND_USER"
    break
  fi

done

if [[ -z "$FOUND_USER" ]]; then
  log_error "Could not find any user producing non-empty candidates with generator '$CANDIDATE_GENERATOR'"
  exit 1
fi

RANK_PAYLOAD=$(echo "$FOUND_CANDIDATES" | jq -c \
  --arg model "$MODEL" \
  --arg did "$FOUND_USER" \
  '{model:$model,user_did:$did,candidates:.candidates}')

log_info "Calling /rank/predict for user $FOUND_USER with model $MODEL"
RANK_RESPONSE=$(curl -sS \
  -H "X-API-Key: $API_KEY" \
  -H 'Content-Type: application/json' \
  "$API_URL/rank/predict" \
  -d "$RANK_PAYLOAD")

RANK_COUNT=$(echo "$RANK_RESPONSE" | jq '.rankings | length' 2>/dev/null || echo 0)

if [[ "$RANK_COUNT" -le 0 ]]; then
  log_error "Rank response was empty"
  echo "$RANK_RESPONSE" | jq . || echo "$RANK_RESPONSE"
  exit 1
fi

log_info "E2E rank test passed"
log_info "Selected user_did: $FOUND_USER"
log_info "Candidates: $(echo "$FOUND_CANDIDATES" | jq '.candidates | length')"
log_info "Rankings: $RANK_COUNT"

echo
jq -n \
  --arg api_url "$API_URL" \
  --arg env "$ENVIRONMENT" \
  --arg user_did "$FOUND_USER" \
  --arg model "$MODEL" \
  --argjson num_candidates "$NUM_CANDIDATES" \
  --argjson rankings "$RANK_COUNT" \
  '{status:"ok",environment:$env,api_url:$api_url,user_did:$user_did,model:$model,num_candidates:$num_candidates,rankings:$rankings}'
