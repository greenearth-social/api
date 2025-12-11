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
ELASTICSEARCH_API_KEY="${ELASTICSEARCH_API_KEY:-your-api-key}"

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

    if [ -n "$ELASTICSEARCH_API_KEY" ] && [ "$ELASTICSEARCH_API_KEY" != "your-api-key" ]; then
        log_info "Elasticsearch API key provided - will be stored/updated in Secret Manager"
    else
        log_warn "Elasticsearch API key not provided - skipping secret creation (assuming it already exists)"
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

    # Cloud Run Invoker - for service-to-service communication
    # gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    #     --member="serviceAccount:$sa_email" \
    #     --role="roles/run.invoker" \
    #     --condition=None

    # If you need to access GCS buckets, add this:
    # gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    #     --member="serviceAccount:$sa_email" \
    #     --role="roles/storage.objectViewer" \
    #     --condition=None

    log_info "IAM roles granted successfully"
}

setup_secrets() {
    log_info "Setting up secrets in Secret Manager..."

    local sa_email="api-runner-$ENVIRONMENT@$PROJECT_ID.iam.gserviceaccount.com"

    # Elasticsearch API key
    if [ -n "$ELASTICSEARCH_API_KEY" ] && [ "$ELASTICSEARCH_API_KEY" != "your-api-key" ]; then
        if ! gcloud secrets describe elasticsearch-api-key > /dev/null 2>&1; then
            echo -n "$ELASTICSEARCH_API_KEY" | gcloud secrets create elasticsearch-api-key --data-file=-
            log_info "Elasticsearch API key secret created."
        else
            log_info "Elasticsearch API key secret already exists. Updating..."
            echo -n "$ELASTICSEARCH_API_KEY" | gcloud secrets versions add elasticsearch-api-key --data-file=-
            log_info "Elasticsearch API key secret updated."
        fi

        # Grant service account access to elasticsearch-api-key
        gcloud secrets add-iam-policy-binding elasticsearch-api-key \
            --member="serviceAccount:$sa_email" \
            --role="roles/secretmanager.secretAccessor" \
            --condition=None
    else
        log_warn "Elasticsearch API key not provided. Skipping secret creation."
        log_info "Ensuring service account has access to existing secret..."
        if gcloud secrets describe elasticsearch-api-key > /dev/null 2>&1; then
            # Grant service account access even if we're not creating/updating the secret
            gcloud secrets add-iam-policy-binding elasticsearch-api-key \
                --member="serviceAccount:$sa_email" \
                --role="roles/secretmanager.secretAccessor" \
                --condition=None 2>/dev/null || log_info "Service account already has access to elasticsearch-api-key"
        else
            log_warn "Elasticsearch API key secret does not exist. You'll need to create it manually or re-run with ELASTICSEARCH_API_KEY set"
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
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --project-id ID          GCP project ID (default: greenearth-471522)"
            echo "  --region REGION          GCP region (default: us-east1)"
            echo "  --environment ENV        Environment name (default: stage)"
            echo "  --help                   Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  PROJECT_ID              Same as --project-id"
            echo "  REGION                  Same as --region"
            echo "  ENVIRONMENT             Same as --environment"
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
