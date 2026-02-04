#!/bin/bash

# Green Earth API - GCP Environment Setup Script
# This script sets up the GCP environment for the API service
# Run this once per environment (stage, prod)

set -e

# Configuration
PROJECT_ID="${PROJECT_ID:-greenearth-471522}"
REGION="${REGION:-us-east1}"
ENVIRONMENT="${ENVIRONMENT:-stage}"

# Elasticsearch configuration - only API key is secret, URL is public
ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-INTERNAL_LB_PLACEHOLDER}"
ELASTICSEARCH_API_KEY="${ELASTICSEARCH_API_KEY:-}"

# API authentication
API_KEY="${API_KEY:-}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
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

check_prerequisites() {
    log_info "Checking prerequisites..."

    if ! command -v gcloud &> /dev/null; then
        log_error "gcloud CLI is not installed. Please install it first."
        exit 1
    fi

    # Check if user is logged in
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -n1 > /dev/null; then
        log_error "Please log in to gcloud first: gcloud auth login"
        exit 1
    fi

    log_info "Prerequisites check complete."
}

validate_config() {
    log_info "Validating configuration..."

    if [ "$PROJECT_ID" = "your-project-id" ]; then
        log_error "Please set PROJECT_ID environment variable or update the script"
        exit 1
    fi

    log_info "Configuration validation complete."
    log_info "Using Elasticsearch URL: $ELASTICSEARCH_URL"

    if [ -n "$ELASTICSEARCH_API_KEY" ]; then
        log_info "Elasticsearch API key provided - will be stored/updated in Secret Manager"
    else
        log_warn "Elasticsearch API key not provided - skipping secret creation (assuming it already exists)"
    fi

    if [ -n "$API_KEY" ]; then
        log_info "API key provided - will be stored/updated in Secret Manager"
    else
        log_warn "API key not provided - skipping secret creation (assuming it already exists)"
    fi
}

setup_gcp_project() {
    log_info "Setting up GCP project: $PROJECT_ID"

    # Set the project
    gcloud config set project "$PROJECT_ID"

    # Enable required APIs
    log_info "Enabling required GCP APIs..."
    gcloud services enable \
        cloudbuild.googleapis.com \
        run.googleapis.com \
        secretmanager.googleapis.com \
        vpcaccess.googleapis.com \
        compute.googleapis.com

    log_info "GCP APIs enabled successfully"
}

create_service_account() {
    log_info "Creating service account for API..."

    local sa_name="api-runner-$ENVIRONMENT"
    local sa_email="$sa_name@$PROJECT_ID.iam.gserviceaccount.com"

    # Check if service account exists
    if gcloud iam service-accounts describe "$sa_email" > /dev/null 2>&1; then
        log_warn "Service account $sa_email already exists"
    else
        gcloud iam service-accounts create "$sa_name" \
            --display-name="Green Earth API Runner - $ENVIRONMENT" \
            --description="Service account for running the Green Earth API on Cloud Run"

        log_info "Service account created: $sa_email"
    fi

    # Grant necessary roles
    log_info "Granting IAM roles to service account..."

    # Secret Manager Secret Accessor - for reading secrets
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$sa_email" \
        --role="roles/secretmanager.secretAccessor" \
        --condition=None

    log_info "IAM roles granted successfully"
}

setup_secrets() {
    log_info "Setting up secrets in Secret Manager..."

    local sa_email="api-runner-$ENVIRONMENT@$PROJECT_ID.iam.gserviceaccount.com"

    # Determine secret names based on environment
    # Stage uses no suffix for backwards compatibility, prod uses -prod suffix
    local es_api_key_secret="elasticsearch-api-key"
    local api_key_secret="api-key"
    if [ "$ENVIRONMENT" = "prod" ]; then
        es_api_key_secret="elasticsearch-api-key-prod"
        api_key_secret="api-key-prod"
    fi

    # Elasticsearch API key
    if [ -n "$ELASTICSEARCH_API_KEY" ] && [ "$ELASTICSEARCH_API_KEY" != "your-api-key" ]; then
        if ! gcloud secrets describe "$es_api_key_secret" > /dev/null 2>&1; then
            echo -n "$ELASTICSEARCH_API_KEY" | gcloud secrets create "$es_api_key_secret" --data-file=-
            log_info "Elasticsearch API key secret created: $es_api_key_secret"
        else
            log_info "Elasticsearch API key secret already exists: $es_api_key_secret. Updating..."
            echo -n "$ELASTICSEARCH_API_KEY" | gcloud secrets versions add "$es_api_key_secret" --data-file=-
            log_info "Elasticsearch API key secret updated: $es_api_key_secret"
        fi

        # Grant service account access to elasticsearch-api-key
        gcloud secrets add-iam-policy-binding "$es_api_key_secret" \
            --member="serviceAccount:$sa_email" \
            --role="roles/secretmanager.secretAccessor" \
            --condition=None
    else
        log_warn "Elasticsearch API key not provided. Skipping secret creation."
        log_info "Ensuring service account has access to existing secret..."
        if gcloud secrets describe "$es_api_key_secret" > /dev/null 2>&1; then
            # Grant service account access even if we're not creating/updating the secret
            gcloud secrets add-iam-policy-binding "$es_api_key_secret" \
                --member="serviceAccount:$sa_email" \
                --role="roles/secretmanager.secretAccessor" \
                --condition=None 2>/dev/null || log_info "Service account already has access to $es_api_key_secret"
        else
            log_warn "Elasticsearch API key secret does not exist: $es_api_key_secret. You'll need to create it manually or re-run with ELASTICSEARCH_API_KEY set"
        fi
    fi

    # API key for authentication
    if [ -n "$API_KEY" ]; then
        if ! gcloud secrets describe "$api_key_secret" > /dev/null 2>&1; then
            echo -n "$API_KEY" | gcloud secrets create "$api_key_secret" --data-file=-
            log_info "API key secret created: $api_key_secret"
        else
            log_info "API key secret already exists: $api_key_secret. Updating..."
            echo -n "$API_KEY" | gcloud secrets versions add "$api_key_secret" --data-file=-
            log_info "API key secret updated: $api_key_secret"
        fi

        # Grant service account access to api-key
        gcloud secrets add-iam-policy-binding "$api_key_secret" \
            --member="serviceAccount:$sa_email" \
            --role="roles/secretmanager.secretAccessor" \
            --condition=None
    else
        log_warn "API key not provided. Skipping secret creation."
        log_info "Ensuring service account has access to existing secret..."
        if gcloud secrets describe "$api_key_secret" > /dev/null 2>&1; then
            gcloud secrets add-iam-policy-binding "$api_key_secret" \
                --member="serviceAccount:$sa_email" \
                --role="roles/secretmanager.secretAccessor" \
                --condition=None 2>/dev/null || log_info "Service account already has access to $api_key_secret"
        else
            log_warn "API key secret does not exist: $api_key_secret. You'll need to create it manually or re-run with API_KEY set"
        fi
    fi

    log_info "Secret setup complete"
}

check_vpc_connector() {
    log_info "Checking for VPC connector..."

    local connector_name="ingex-vpc-connector-$ENVIRONMENT"

    if gcloud compute networks vpc-access connectors describe "$connector_name" --region="$REGION" > /dev/null 2>&1; then
        log_info "VPC connector '$connector_name' already exists"
        log_info "API will be able to use this for internal network access"
    else
        log_warn "VPC connector '$connector_name' does not exist"
        log_warn "If you need internal network access (e.g., to Elasticsearch), run:"
        log_warn "  cd ../ingex/ingest && ./scripts/gcp_setup.sh"
        log_warn ""
        log_warn "The API can still be deployed without VPC connector for public-only access"
    fi
}

fetch_elasticsearch_api_key() {
    # Fetch the ES API key from Kubernetes and store it in Secret Manager
    # This connects to the K8s cluster, creates an API key via the ES API,
    # and stores it in GCP Secret Manager

    log_info "Fetching Elasticsearch API key from Kubernetes cluster..."

    local k8s_namespace="greenearth-$ENVIRONMENT"
    local k8s_cluster="greenearth-$ENVIRONMENT-cluster"

    # Determine secret name based on environment
    local es_api_key_secret="elasticsearch-api-key"
    if [ "$ENVIRONMENT" = "prod" ]; then
        es_api_key_secret="elasticsearch-api-key-prod"
    fi

    # Check if kubectl is available
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl is not installed. Cannot fetch ES API key from K8s."
        log_error "Install kubectl or provide ELASTICSEARCH_API_KEY manually."
        return 1
    fi

    # Set up kubectl context for the target environment
    log_info "Setting kubectl context for $ENVIRONMENT environment..."
    if ! gcloud container clusters get-credentials "$k8s_cluster" \
        --location="$REGION" \
        --project="$PROJECT_ID" 2>/dev/null; then
        log_error "Failed to get K8s credentials. Is the cluster deployed?"
        return 1
    fi

    # Get elastic superuser credentials (required for creating API keys)
    log_info "Retrieving elastic superuser password..."
    local es_password
    es_password=$(kubectl get secret greenearth-es-elastic-user -n "$k8s_namespace" -o jsonpath='{.data.elastic}' 2>/dev/null | base64 -d)

    if [ -z "$es_password" ]; then
        log_error "Could not retrieve elastic superuser password from K8s secret."
        log_error "Is Elasticsearch deployed in namespace $k8s_namespace?"
        return 1
    fi

    # Determine ES pod name based on environment
    local es_pod
    if [ "$ENVIRONMENT" = "prod" ]; then
        es_pod="greenearth-es-data-0"
    else
        es_pod="greenearth-es-data-only-0"
    fi

    log_info "Creating API key via Elasticsearch API (pod: $es_pod)..."

    # Create API key with permissions for the API service
    local api_key_response
    api_key_response=$(kubectl exec -n "$k8s_namespace" "$es_pod" -- curl -k -s -X POST \
        -u "elastic:$es_password" \
        "https://localhost:9200/_security/api_key" \
        -H "Content-Type: application/json" \
        -d '{
            "name": "greenearth-api-'"$ENVIRONMENT"'",
            "expiration": "365d",
            "role_descriptors": {
                "api_role": {
                    "cluster": ["monitor"],
                    "indices": [
                        {
                            "names": ["posts", "posts_*", "likes", "likes_*", "hashtags", "hashtags_*"],
                            "privileges": ["read", "view_index_metadata"]
                        }
                    ]
                }
            }
        }')

    # Extract the encoded API key
    local encoded_key
    encoded_key=$(echo "$api_key_response" | grep -o '"encoded":"[^"]*"' | cut -d'"' -f4)

    if [ -z "$encoded_key" ]; then
        log_error "Failed to create API key. Response: $api_key_response"
        return 1
    fi

    log_info "API key created successfully."

    # Store in Secret Manager
    log_info "Storing API key in Secret Manager ($es_api_key_secret)..."
    if ! gcloud secrets describe "$es_api_key_secret" > /dev/null 2>&1; then
        echo -n "$encoded_key" | gcloud secrets create "$es_api_key_secret" --data-file=-
        log_info "Secret created: $es_api_key_secret"
    else
        echo -n "$encoded_key" | gcloud secrets versions add "$es_api_key_secret" --data-file=-
        log_info "Secret updated: $es_api_key_secret"
    fi

    # Export for use in setup_secrets
    ELASTICSEARCH_API_KEY="$encoded_key"
    log_info "Elasticsearch API key fetched and stored successfully."
    return 0
}

main() {
    log_info "Starting GCP setup for Green Earth API..."
    log_info "Project: $PROJECT_ID"
    log_info "Region: $REGION"
    log_info "Environment: $ENVIRONMENT"
    echo ""

    check_prerequisites
    validate_config
    setup_gcp_project
    create_service_account

    # Fetch ES API key from K8s unless disabled or already provided
    if [ "$FETCH_ES_KEY" = true ] && [ -z "$ELASTICSEARCH_API_KEY" ]; then
        if ! fetch_elasticsearch_api_key; then
            log_warn "Failed to fetch ES API key. Continuing with setup..."
        fi
    elif [ -n "$ELASTICSEARCH_API_KEY" ]; then
        log_info "Using provided ELASTICSEARCH_API_KEY (skipping K8s fetch)"
    fi

    setup_secrets
    check_vpc_connector

    echo ""
    log_info "✓ GCP setup complete!"
    echo ""
    log_info "Next steps:"
    log_info "  1. Review and configure secrets in Secret Manager if needed"
    log_info "  2. Run ./scripts/deploy.sh to deploy the API to Cloud Run"
    echo ""
}

# Parse command line arguments
FETCH_ES_KEY=true
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
        --no-fetch-es-key)
            FETCH_ES_KEY=false
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --project-id ID          GCP project ID (default: greenearth-471522)"
            echo "  --region REGION          GCP region (default: us-east1)"
            echo "  --environment ENV        Environment name (default: stage)"
            echo "  --no-fetch-es-key        Skip fetching ES API key from K8s (use existing secret or env var)"
            echo "  --help                   Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  PROJECT_ID              Same as --project-id"
            echo "  REGION                  Same as --region"
            echo "  ENVIRONMENT             Same as --environment"
            echo "  API_KEY                 API key for authentication (stored in Secret Manager)"
            echo "  ELASTICSEARCH_API_KEY   Elasticsearch API key (skips K8s fetch if provided)"
            echo ""
            echo "Examples:"
            echo "  # Setup for staging (fetches ES key from K8s by default):"
            echo "  $0 --environment stage"
            echo ""
            echo "  # Setup for production with manual ES key:"
            echo "  ELASTICSEARCH_API_KEY=xxx $0 --environment prod --no-fetch-es-key"
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
