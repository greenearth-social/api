#!/bin/bash

# Green Earth API - Cloud Run Source Deployment Script
# This script deploys the FastAPI service to Google Cloud Run using source deployment
# with environment-specific service names (greenearth-api-stage, greenearth-api-prod)
# Source deployment uses Google Cloud buildpacks to automatically build from Python source
#
# Prerequisites: Run scripts/gcp_setup.sh first to configure the GCP environment

set -e

# Configuration
PROJECT_ID="${PROJECT_ID:-greenearth-471522}"
REGION="${REGION:-us-east1}"
ENVIRONMENT="${ENVIRONMENT:-stage}"

# Elasticsearch configuration
ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-INTERNAL_LB_PLACEHOLDER}"

# Service configuration
API_INSTANCES_MIN="${API_INSTANCES_MIN:-1}"
API_INSTANCES_MAX="${API_INSTANCES_MAX:-10}"
API_REQUEST_TIMEOUT="${API_REQUEST_TIMEOUT:-60}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_build() {
    echo -e "${BLUE}[BUILD]${NC} $1"
}

validate_config() {
    log_info "Validating configuration..."

    if [ "$PROJECT_ID" = "your-project-id" ]; then
        log_error "Please set PROJECT_ID environment variable or use --project-id"
        exit 1
    fi

    # Set gcloud project
    gcloud config set project "$PROJECT_ID"

    log_info "Configuration validation complete."
}

configure_kubectl() {
    log_info "Configuring kubectl context for $ENVIRONMENT environment..."

    local cluster_name="greenearth-${ENVIRONMENT}-cluster"

    if ! gcloud container clusters get-credentials "$cluster_name" \
        --location="$REGION" \
        --project="$PROJECT_ID" 2>/dev/null; then
        log_warn "Could not configure kubectl for cluster $cluster_name"
        log_warn "If you need to auto-detect Elasticsearch URL, set ELASTICSEARCH_URL manually"
        return 1
    fi

    log_info "kubectl configured for cluster: $cluster_name"
    return 0
}

get_elasticsearch_internal_lb_ip() {
    log_info "Getting Elasticsearch internal load balancer IP..."

    # If user has explicitly set a URL, use it
    if [ "$ELASTICSEARCH_URL" != "INTERNAL_LB_PLACEHOLDER" ]; then
        log_info "Using user-provided Elasticsearch URL: $ELASTICSEARCH_URL"
        return
    fi

    # Try to get the internal load balancer IP from the Kubernetes service
    # This assumes the load balancer has been deployed and has an assigned IP
    if command -v kubectl &> /dev/null; then
        local lb_ip
        lb_ip=$(kubectl get service greenearth-es-internal-lb -n "greenearth-$ENVIRONMENT" -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")

        if [ -n "$lb_ip" ] && [ "$lb_ip" != "null" ]; then
            # Use the internal load balancer IP
            ELASTICSEARCH_URL="https://$lb_ip:9200"
            log_info "Using internal load balancer IP: $ELASTICSEARCH_URL"
            log_warn "Note: Certificate verification may fail for IP-based connections"
            log_warn "Services should be configured to skip certificate verification for internal LB"
        else
            log_warn "Could not get internal load balancer IP"
            log_warn "Make sure the Elasticsearch cluster is deployed with internal load balancer"
            log_error "Please deploy Elasticsearch cluster first or set ELASTICSEARCH_URL manually"
            exit 1
        fi
    else
        log_error "kubectl not available - cannot determine Elasticsearch internal load balancer IP"
        log_error "Please install kubectl or set ELASTICSEARCH_URL manually"
        exit 1
    fi
}

verify_vpc_connector() {
    log_info "Verifying VPC connector exists..."

    CONNECTOR_NAME="ingex-vpc-connector-$ENVIRONMENT"

    if ! gcloud compute networks vpc-access connectors describe "$CONNECTOR_NAME" --region="$REGION" > /dev/null 2>&1; then
        log_warn "VPC connector '$CONNECTOR_NAME' does not exist"
        log_warn "Deploying without VPC connector - service will not be able to access internal resources"
        log_warn "Run ../ingex/ingest/scripts/gcp_setup.sh to create VPC connector if needed"
        VPC_CONNECTOR_EXISTS=false
    else
        # Check connector status
        local connector_status=$(gcloud compute networks vpc-access connectors describe "$CONNECTOR_NAME" --region="$REGION" --format="value(state)" 2>/dev/null || echo "UNKNOWN")

        if [ "$connector_status" != "READY" ]; then
            log_warn "VPC connector '$CONNECTOR_NAME' is not ready (status: $connector_status)"
            log_warn "This may cause deployment to fail. Wait a few minutes and try again."
        else
            log_info "VPC connector '$CONNECTOR_NAME' is ready"
        fi
        VPC_CONNECTOR_EXISTS=true
    fi
}

generate_requirements() {
    log_info "Generating requirements.txt from Pipfile..."

    if ! command -v pipenv &> /dev/null; then
        log_error "pipenv is not installed. Please install it first: pip install pipenv"
        exit 1
    fi

    # Generate requirements.txt for buildpacks
    pipenv requirements > requirements.txt

    if [ $? -eq 0 ]; then
        log_info "Generated requirements.txt successfully"
    else
        log_error "Failed to generate requirements.txt"
        exit 1
    fi
}

deploy_firestore_config() {
    log_info "Deploying Firestore rules and indexes for project $PROJECT_ID..."

    if ! command -v firebase &> /dev/null; then
        log_error "firebase CLI is not installed. Install with: npm install -g firebase-tools"
        exit 1
    fi

    if [ ! -f "firebase.json" ] || [ ! -f "firestore.rules" ] || [ ! -f "firestore.indexes.json" ]; then
        log_error "Missing firebase.json, firestore.rules, or firestore.indexes.json in $(pwd)"
        log_error "Run this script from the api/ directory where Firebase config lives"
        exit 1
    fi

    # Deploy Firestore config idempotently: applies rules and creates/updates indexes as needed.
    firebase deploy --only firestore --project "$PROJECT_ID"

    if [ $? -eq 0 ]; then
        log_info "✓ Firestore rules and indexes deployed successfully"
    else
        log_error "Failed to deploy Firestore rules/indexes"
        exit 1
    fi
}

deploy_api_service() {
    log_info "Deploying greenearth-api-$ENVIRONMENT service from source..."

    # Determine secret names based on environment
    # Stage uses no suffix for backwards compatibility, prod uses -prod suffix
    # API uses the readonly key since it only needs read access to Elasticsearch
    local es_api_key_secret="elasticsearch-api-key-readonly"
    local api_key_secret="api-key"
    local firestore_api_key_secret="firestore-api-key-stage"
    local firestore_database="greenearth-stage"
    if [ "$ENVIRONMENT" = "prod" ]; then
        es_api_key_secret="elasticsearch-api-key-readonly-prod"
        api_key_secret="api-key-prod"
        firestore_api_key_secret="firestore-api-key-prod"
        firestore_database="greenearth-prod"
    fi

    # Build base command with environment suffix in service name
    local deploy_cmd="gcloud run deploy greenearth-api-$ENVIRONMENT"
    deploy_cmd="$deploy_cmd --source=."
    deploy_cmd="$deploy_cmd --region=$REGION"
    deploy_cmd="$deploy_cmd --service-account=api-runner-$ENVIRONMENT@$PROJECT_ID.iam.gserviceaccount.com"

    # Add VPC connector if it exists
    if [ "$VPC_CONNECTOR_EXISTS" = true ]; then
        deploy_cmd="$deploy_cmd --vpc-connector=ingex-vpc-connector-$ENVIRONMENT"
        deploy_cmd="$deploy_cmd --vpc-egress=private-ranges-only"
    fi

    # Set environment variables
    deploy_cmd="$deploy_cmd --set-env-vars=ENVIRONMENT=$ENVIRONMENT"
    deploy_cmd="$deploy_cmd --set-env-vars=LOG_LEVEL=info"
    deploy_cmd="$deploy_cmd --set-env-vars=GE_ELASTICSEARCH_URL=$ELASTICSEARCH_URL"
    deploy_cmd="$deploy_cmd --set-env-vars=GE_ELASTICSEARCH_VERIFY_SSL=false"
    deploy_cmd="$deploy_cmd --set-env-vars=GE_FIRESTORE_PROJECT=$PROJECT_ID"
    deploy_cmd="$deploy_cmd --set-env-vars=GE_FIRESTORE_DATABASE=$firestore_database"

    # Add secrets with environment-specific names
    deploy_cmd="$deploy_cmd --set-secrets=GE_ELASTICSEARCH_API_KEY=$es_api_key_secret:latest"
    deploy_cmd="$deploy_cmd --set-secrets=API_KEY=$api_key_secret:latest"
    deploy_cmd="$deploy_cmd --set-secrets=GE_FIRESTORE_API_KEY=$firestore_api_key_secret:latest"

    # Resource and scaling configuration
    deploy_cmd="$deploy_cmd --min-instances=$API_INSTANCES_MIN"
    deploy_cmd="$deploy_cmd --max-instances=$API_INSTANCES_MAX"
    deploy_cmd="$deploy_cmd --cpu=1"
    deploy_cmd="$deploy_cmd --memory=512Mi"
    deploy_cmd="$deploy_cmd --timeout=$API_REQUEST_TIMEOUT"
    deploy_cmd="$deploy_cmd --concurrency=80"

    # Allow unauthenticated access (adjust based on your needs)
    deploy_cmd="$deploy_cmd --allow-unauthenticated"

    log_build "Executing: $deploy_cmd"
    eval "$deploy_cmd"

    if [ $? -eq 0 ]; then
        log_info "✓ greenearth-api-$ENVIRONMENT deployed successfully"

        # Get the service URL
        local service_url=$(gcloud run services describe greenearth-api-$ENVIRONMENT --region="$REGION" --project="$PROJECT_ID" --format="value(status.url)")
        log_info "Service URL: $service_url"

        # Ensure runtime DID is explicitly set so /.well-known/did.json is env-correct
        local generator_did
        if [ "$ENVIRONMENT" = "prod" ]; then
            generator_did="did:web:api.greenearth.social"
        else
            local service_host
            service_host=$(echo "$service_url" | sed 's|https://||')
            generator_did="did:web:$service_host"
        fi

        gcloud run services update "greenearth-api-$ENVIRONMENT" \
            --region="$REGION" \
            --project="$PROJECT_ID" \
            --update-env-vars="GE_FEED_GENERATOR_DID=$generator_did" \
            --remove-env-vars="GE_FIRESTORE_EMULATOR_HOST,FIRESTORE_EMULATOR_HOST" > /dev/null
        log_info "Set GE_FEED_GENERATOR_DID=$generator_did"
    else
        log_error "Failed to deploy greenearth-api-$ENVIRONMENT"
        exit 1
    fi
}

resolve_generator_did() {
    if [ "$ENVIRONMENT" = "prod" ]; then
        echo "did:web:api.greenearth.social"
        return 0
    fi

    local service_url
    service_url=$(gcloud run services describe "greenearth-api-$ENVIRONMENT" \
        --region="$REGION" --project="$PROJECT_ID" --format="value(status.url)" 2>/dev/null)
    if [ -z "$service_url" ]; then
        return 1
    fi

    local service_host
    service_host=$(echo "$service_url" | sed 's|https://||')
    echo "did:web:$service_host"
}

sync_feeds() {
    log_info "Syncing feed generator records for $ENVIRONMENT..."

    # Determine Bluesky handle and secret name for this environment
    local bsky_handle
    local bsky_secret
    if [ "$ENVIRONMENT" = "prod" ]; then
        bsky_handle="greenearth-social.bsky.social"
        bsky_secret="bsky-app-password-prod"
    else
        bsky_handle="ge-stage.bsky.social"
        bsky_secret="bsky-app-password"
    fi

    # Fetch app password from Secret Manager
    local bsky_password
    bsky_password=$(gcloud secrets versions access latest --secret="$bsky_secret" --project="$PROJECT_ID" 2>/dev/null)
    if [ -z "$bsky_password" ]; then
        log_warn "Could not fetch Bluesky app password from secret '$bsky_secret'"
        log_warn "Skipping feed sync. Store the password with:"
        log_warn "  echo -n '<password>' | gcloud secrets create $bsky_secret --data-file=- --project=$PROJECT_ID"
        return 0
    fi

    local generator_did
    if ! generator_did=$(resolve_generator_did); then
        log_warn "Could not determine service URL — skipping feed sync"
        return 0
    fi

    log_info "Handle:        $bsky_handle"
    log_info "Generator DID: $generator_did"

    pipenv run python scripts/publish_feed.py \
        --handle "$bsky_handle" \
        --app-password "$bsky_password" \
        --generator-did "$generator_did" \
        --environment "$ENVIRONMENT" \
        --sync

    if [ $? -eq 0 ]; then
        log_info "Feed records synced successfully"
    else
        log_warn "Feed sync failed — feeds may be out of date"
    fi
}

main() {
    log_info "Starting Green Earth API deployment..."
    log_info "Project: $PROJECT_ID"
    log_info "Region: $REGION"
    log_info "Environment: $ENVIRONMENT"

    validate_config
    verify_vpc_connector

    # Configure kubectl if needed for ES URL auto-detection
    if [ "$ELASTICSEARCH_URL" = "INTERNAL_LB_PLACEHOLDER" ]; then
        configure_kubectl
    fi

    get_elasticsearch_internal_lb_ip
    deploy_firestore_config
    generate_requirements
    deploy_api_service
    sync_feeds

    log_info "Deployment complete!"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
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
        --min-instances)
            API_INSTANCES_MIN="$2"
            shift 2
            ;;
        --max-instances)
            API_INSTANCES_MAX="$2"
            shift 2
            ;;
        --timeout)
            API_REQUEST_TIMEOUT="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --project-id ID          GCP project ID (default: greenearth-471522)"
            echo "  --region REGION          GCP region (default: us-east1)"
            echo "  --environment ENV        Environment name (default: stage)"
            echo "  --min-instances N        Minimum instances (default: 1)"
            echo "  --max-instances N        Maximum instances (default: 10)"
            echo "  --timeout SECONDS        Cloud Run request timeout (default: 60)"
            echo "  --help                   Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  PROJECT_ID              Same as --project-id"
            echo "  REGION                  Same as --region"
            echo "  ENVIRONMENT             Same as --environment"
            echo "  API_REQUEST_TIMEOUT     Same as --timeout"
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

main
