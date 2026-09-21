#!/bin/bash

# Green Earth API - Cloud Run Source Deployment Script
# This script deploys the FastAPI service to Google Cloud Run using source deployment
# with environment-specific service names (greenearth-api-stage, greenearth-api-prod)
# Source deployment uses Google Cloud buildpacks to automatically build from Python source
#
# Prerequisites: Run scripts/gcp_setup.sh first to configure the GCP environment

set -e

# Configuration (overridden by CLI args)
PROJECT_ID="greenearth-471522"
REGION="us-east1"
ENVIRONMENT="stage"

# Elasticsearch configuration
GE_ELASTICSEARCH_URL="INTERNAL_LB_PLACEHOLDER"

# Index two-tower kNN searches. posts_recent_quality holds only posts at or
# above MIN_LIKE_COUNT, so that filter is non-selective there and Lucene keeps
# using the HNSW graph instead of exact-scanning every matching vector
# (greenearth-social/ingex#442). Set explicitly rather than relying on the app
# default so the deployed value is visible on the revision. Override with
# GE_TWO_TOWER_KNN_INDEX=posts_recent to fall back to the full corpus — needed
# in any environment where the quality corpus has not been backfilled yet
# (ingex ingest/cmd/backfill_quality_index).
GE_TWO_TOWER_KNN_INDEX="${GE_TWO_TOWER_KNN_INDEX:-posts_recent_quality}"

# Inference configuration
GE_INFERENCE_BASE_URL=""

# Frontend origin used by Settings links in repository-managed Bluesky posts.
# Keep stage aligned with the APP_ORIGIN deployed to the stage OAuth functions.
GE_SETTINGS_APP_ORIGIN="${GE_SETTINGS_APP_ORIGIN:-}"
GE_SETTINGS_APP_METADATA_URL="${GE_SETTINGS_APP_METADATA_URL:-}"
GE_SETTINGS_LINK_ORIGIN="${GE_SETTINGS_LINK_ORIGIN:-}"
STAGE_SETTINGS_APP_ORIGIN="https://greenearth-471522--stage-4tnzb2wq.web.app"
STAGE_SETTINGS_APP_METADATA_URL="https://us-central1-greenearth-471522.cloudfunctions.net/oauthClientMetadataStage"
STAGE_SETTINGS_LINK_ORIGIN="https://greenearth-api-stage-oef7fsaama-ue.a.run.app"
PROD_SETTINGS_APP_ORIGIN="https://app.greenearth.social"

# Short git sha of the deployed code, resolved by require_clean_worktree().
# Stamped onto the Cloud Run revision (env var + label) and onto debug feed
# records so we can identify exactly what code is live (see issue #228).
GIT_SHA=""

# UX posts (feed pins, the survey post, the logged-out explainer) are resolved into
# src/app/ux_posts_resolved.json before the source upload. That manifest is generated
# and gitignored, so it never conflicts between branches, but it ships inside the
# image -- which is what makes a rollback resolve the URIs it was built with.
SKIP_UX_POST_SYNC=false

# MySky's first-time and Explore posts are externally published native-video
# records. Reuse configured values, or recover them from recent revisions.
GE_PINNED_POST_YOUR_FEED_URI="${GE_PINNED_POST_YOUR_FEED_URI:-}"
GE_PINNED_POST_YOUR_FEED_EXPLORE_URI="${GE_PINNED_POST_YOUR_FEED_EXPLORE_URI:-}"

# Bluesky publishing identities. Use stable account DIDs for authentication so
# account handle changes cannot break deployments. Caterpie's environment-specific
# app-password secrets belong to the same account.
PROD_BSKY_PUBLISHER_ID="did:plc:wrmpulygwvuhjn2c3jbalgqj"
PROD_BSKY_SECRET="bsky-app-password-prod"
CATERPIE_BSKY_PUBLISHER_ID="did:plc:s4tl2ajfsnstzuxtegl7r33g"
CATERPIE_STAGE_BSKY_SECRET="bsky-app-password-caterpie"
CATERPIE_PROD_BSKY_SECRET="bsky-app-password-caterpie-prod"
# UX posts publish to the notifications account rather than the brand account, so
# republished revisions never reach the brand account's followers (issue #404). Both
# environments share it: the AppView hydrates any public URI regardless of which
# generator served the skeleton.
NOTIFY_BSKY_PUBLISHER_ID="did:plc:66mudnfk2p4olwpaskmrw2vq"
NOTIFY_BSKY_SECRET="bsky-app-password-notify-prod"

# PostHog configuration. Each environment is a separate PostHog project (separate
# API key, provisioned via scripts/gcp_setup.sh), but all projects live on the same
# PostHog Cloud host, so the host is a constant here rather than a per-env secret.
GE_POSTHOG_HOST="https://us.i.posthog.com"

# Service configuration
API_INSTANCES_MIN="1"
# Sized from prod telemetry (issue #389): a CPU-vs-concurrency regression over
# two load tests puts this 1-vCPU instance's saturation point at ~25 concurrent
# requests; --concurrency below is set well under that. Cutting concurrency
# from its previous 80 quarters the per-instance capacity, so the ceiling here
# is raised to compensate -- CPU-throughput math (~0.34 CPU-s/request) says the
# already-observed peak (460 req/min) needs only ~4 instances at a sane
# per-instance utilization target, so 20 leaves several times that headroom.
API_INSTANCES_MAX="20"
API_REQUEST_TIMEOUT="60"

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

default_inference_domain() {
    if [ "$ENVIRONMENT" = "prod" ]; then
        echo "inference.greenearth.social"
    else
        echo "inference-stage.greenearth.social"
    fi
}

resolve_inference_base_url() {
    local inference_domain
    inference_domain="$(default_inference_domain)"
    GE_INFERENCE_BASE_URL="https://$inference_domain"
    log_info "Using mapped inference URL: $GE_INFERENCE_BASE_URL"
}

resolve_settings_app_origin() {
    if [ -z "$GE_SETTINGS_APP_ORIGIN" ]; then
        if [ "$ENVIRONMENT" = "prod" ]; then
            GE_SETTINGS_APP_ORIGIN="$PROD_SETTINGS_APP_ORIGIN"
        else
            GE_SETTINGS_APP_ORIGIN="$STAGE_SETTINGS_APP_ORIGIN"
        fi
    fi
    if [ "$ENVIRONMENT" != "prod" ]; then
        if [ -z "$GE_SETTINGS_APP_METADATA_URL" ]; then
            GE_SETTINGS_APP_METADATA_URL="$STAGE_SETTINGS_APP_METADATA_URL"
        fi
        if [ -z "$GE_SETTINGS_LINK_ORIGIN" ]; then
            GE_SETTINGS_LINK_ORIGIN=$(gcloud run services describe \
                "greenearth-api-$ENVIRONMENT" \
                --region="$REGION" \
                --project="$PROJECT_ID" \
                --format="value(status.url)" 2>/dev/null || true)
            GE_SETTINGS_LINK_ORIGIN="${GE_SETTINGS_LINK_ORIGIN:-$STAGE_SETTINGS_LINK_ORIGIN}"
        fi
    fi
    GE_SETTINGS_APP_ORIGIN="${GE_SETTINGS_APP_ORIGIN%/}"
    GE_SETTINGS_LINK_ORIGIN="${GE_SETTINGS_LINK_ORIGIN%/}"
    export GE_SETTINGS_APP_ORIGIN GE_SETTINGS_APP_METADATA_URL GE_SETTINGS_LINK_ORIGIN
    log_info "Using Settings app origin: $GE_SETTINGS_APP_ORIGIN"
    if [ -n "$GE_SETTINGS_LINK_ORIGIN" ]; then
        log_info "Publishing stable Settings links through: $GE_SETTINGS_LINK_ORIGIN"
    fi
}

require_clean_worktree() {
    log_info "Verifying git working tree is clean..."

    if ! git rev-parse --git-dir > /dev/null 2>&1; then
        log_error "Not inside a git repository — cannot verify the deployed code."
        log_error "Run deploy.sh from a checkout of the api repo."
        exit 1
    fi

    # Refuse to deploy with uncommitted changes so the stamped git sha always
    # matches the code that ships. Deploying an unpushed branch is fine — only a
    # dirty tree is rejected (see issue #228).
    if [ -n "$(git status --porcelain)" ]; then
        log_error "Working tree has uncommitted changes. Commit or stash them before deploying"
        log_error "so the deployed git sha reflects the running code."
        git status --short
        exit 1
    fi

    GIT_SHA="$(git rev-parse --short=7 HEAD)"
    export GE_GIT_SHA="$GIT_SHA"  # picked up by publish_feed.py during feed sync
    log_info "Deploying git sha: $GIT_SHA ($(git rev-parse --abbrev-ref HEAD))"
}

validate_config() {
    log_info "Validating configuration..."

    if [ "$PROJECT_ID" = "your-project-id" ]; then
        log_error "Please set --project-id"
        exit 1
    fi

    # Set gcloud project
    gcloud config set project "$PROJECT_ID"

    resolve_inference_base_url
    resolve_settings_app_origin

    log_info "Configuration validation complete."
}

configure_kubectl() {
    log_info "Configuring kubectl context for $ENVIRONMENT environment..."

    local cluster_name="greenearth-${ENVIRONMENT}-cluster"

    if ! gcloud container clusters get-credentials "$cluster_name" \
        --location="$REGION" \
        --project="$PROJECT_ID" 2>/dev/null; then
        log_warn "Could not configure kubectl for cluster $cluster_name"
        log_warn "If you need to set Elasticsearch URL manually, use --elasticsearch-url"
        return 1
    fi

    log_info "kubectl configured for cluster: $cluster_name"
    return 0
}

get_elasticsearch_internal_lb_ip() {
    log_info "Getting Elasticsearch internal load balancer IP..."

    # If user has explicitly set a URL, use it
    if [ "$GE_ELASTICSEARCH_URL" != "INTERNAL_LB_PLACEHOLDER" ]; then
        log_info "Using user-provided Elasticsearch URL: $GE_ELASTICSEARCH_URL"
        return
    fi

    # Try to get the internal load balancer IP from the Kubernetes service
    # This assumes the load balancer has been deployed and has an assigned IP
    if command -v kubectl &> /dev/null; then
        local lb_ip
        lb_ip=$(kubectl get service greenearth-es-internal-lb -n "greenearth-$ENVIRONMENT" -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")

        if [ -n "$lb_ip" ] && [ "$lb_ip" != "null" ]; then
            # Use the internal load balancer IP
            GE_ELASTICSEARCH_URL="https://$lb_ip:9200"
            log_info "Using internal load balancer IP: $GE_ELASTICSEARCH_URL"
            log_warn "Note: Certificate verification may fail for IP-based connections"
            log_warn "Services should be configured to skip certificate verification for internal LB"
        else
            log_warn "Could not get internal load balancer IP"
            log_warn "Make sure the Elasticsearch cluster is deployed with internal load balancer"
            log_error "Please deploy Elasticsearch cluster first or pass --elasticsearch-url"
            exit 1
        fi
    else
        log_error "kubectl not available - cannot determine Elasticsearch internal load balancer IP"
        log_error "Please install kubectl or pass --elasticsearch-url"
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

deploy_api_service() {
    log_info "Deploying greenearth-api-$ENVIRONMENT service from source..."

    # Determine secret names based on environment
    # Stage uses no suffix for backwards compatibility, prod uses -prod suffix
    # API uses the readonly key since it only needs read access to Elasticsearch
    local es_api_key_secret="elasticsearch-api-key-readonly"
    local inference_api_key_secret="inference-api-key-stage"
    local firestore_api_key_secret="firestore-api-key-stage"
    local feed_context_secret="feed-context-secret-stage"
    local probe_secret="probe-secret-stage"
    local load_test_secret="load-test-secret-stage"
    local perspective_api_key_secret="perspective-api-key-stage"
    local posthog_api_key_secret="posthog-api-key-stage"
    local firestore_database="greenearth-stage"
    if [ "$ENVIRONMENT" = "prod" ]; then
        es_api_key_secret="elasticsearch-api-key-readonly-prod"
        inference_api_key_secret="inference-api-key-prod"
        firestore_api_key_secret="firestore-api-key-prod"
        feed_context_secret="feed-context-secret-prod"
        probe_secret="probe-secret-prod"
        load_test_secret="load-test-secret-prod"
        perspective_api_key_secret="perspective-api-key-prod"
        posthog_api_key_secret="posthog-api-key-prod"
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

    # Set environment variables. The app derives its default log level from
    # ENVIRONMENT (stage/prod -> WARNING, else INFO). Override with GE_LOG_LEVEL.
    deploy_cmd="$deploy_cmd --set-env-vars=ENVIRONMENT=$ENVIRONMENT"
    # Stamp the deployed git sha so the app can report it (e.g. /health) and so
    # revisions are identifiable for rollbacks (see issue #228).
    deploy_cmd="$deploy_cmd --set-env-vars=GE_GIT_SHA=$GIT_SHA"
    deploy_cmd="$deploy_cmd --set-env-vars=GE_ELASTICSEARCH_URL=$GE_ELASTICSEARCH_URL"
    deploy_cmd="$deploy_cmd --set-env-vars=GE_ELASTICSEARCH_VERIFY_SSL=false"
    deploy_cmd="$deploy_cmd --set-env-vars=GE_TWO_TOWER_KNN_INDEX=$GE_TWO_TOWER_KNN_INDEX"
    deploy_cmd="$deploy_cmd --set-env-vars=GE_FIRESTORE_PROJECT=$PROJECT_ID"
    deploy_cmd="$deploy_cmd --set-env-vars=GE_FIRESTORE_DATABASE=$firestore_database"
    deploy_cmd="$deploy_cmd --set-env-vars=GE_SETTINGS_APP_ORIGIN=$GE_SETTINGS_APP_ORIGIN"
    deploy_cmd="$deploy_cmd --set-env-vars=GE_SETTINGS_APP_METADATA_URL=$GE_SETTINGS_APP_METADATA_URL"
    deploy_cmd="$deploy_cmd --set-env-vars=GE_SETTINGS_LINK_ORIGIN=$GE_SETTINGS_LINK_ORIGIN"
    deploy_cmd="$deploy_cmd --set-env-vars=GE_PROBE_USER_DID=did:plc:s4tl2ajfsnstzuxtegl7r33g"
    deploy_cmd="$deploy_cmd --set-env-vars=GE_CANDIDATE_GENERATOR_TIMEOUT_SEC=4"
    deploy_cmd="$deploy_cmd --set-env-vars=GE_RANK_MODEL_TIMEOUT_SEC=2.5"
    deploy_cmd="$deploy_cmd --set-env-vars=GE_EMBED_HYDRATION_TIMEOUT_SEC=1.5"
    deploy_cmd="$deploy_cmd --set-env-vars=GE_PINNED_POST_YOUR_FEED_URI=$GE_PINNED_POST_YOUR_FEED_URI"
    deploy_cmd="$deploy_cmd --set-env-vars=GE_PINNED_POST_YOUR_FEED_EXPLORE_URI=$GE_PINNED_POST_YOUR_FEED_EXPLORE_URI"
    # Below the AppView's 10s abort on getFeedSkeleton calls (confirmed via
    # atproto source, see #291) so a hung downstream call (ES, ranker)
    # surfaces as a logged, metered 504 instead of losing the race against
    # the client's own timeout with nothing recorded on our side (see #270).
    deploy_cmd="$deploy_cmd --set-env-vars=GE_FEED_REQUEST_TIMEOUT_SEC=9"
    deploy_cmd="$deploy_cmd --set-env-vars=GE_POSTHOG_HOST=$GE_POSTHOG_HOST"
    if [ -n "$GE_INFERENCE_BASE_URL" ]; then
        deploy_cmd="$deploy_cmd --set-env-vars=GE_INFERENCE_BASE_URL=$GE_INFERENCE_BASE_URL"
    fi

    # Add secrets with environment-specific names
    deploy_cmd="$deploy_cmd --set-secrets=GE_ELASTICSEARCH_API_KEY=$es_api_key_secret:latest"
    deploy_cmd="$deploy_cmd --set-secrets=GE_INFERENCE_API_KEY=$inference_api_key_secret:latest"
    deploy_cmd="$deploy_cmd --set-secrets=GE_FIRESTORE_API_KEY=$firestore_api_key_secret:latest"
    deploy_cmd="$deploy_cmd --set-secrets=GE_FEED_CONTEXT_SECRET=$feed_context_secret:latest"
    deploy_cmd="$deploy_cmd --set-secrets=GE_PROBE_SECRET=$probe_secret:latest"
    deploy_cmd="$deploy_cmd --set-secrets=GE_LOAD_TEST_SECRET=$load_test_secret:latest"
    deploy_cmd="$deploy_cmd --set-secrets=GE_PERSPECTIVE_API_KEY=$perspective_api_key_secret:latest"
    deploy_cmd="$deploy_cmd --set-secrets=GE_POSTHOG_API_KEY=$posthog_api_key_secret:latest"

    # Resource and scaling configuration
    deploy_cmd="$deploy_cmd --min-instances=$API_INSTANCES_MIN"
    deploy_cmd="$deploy_cmd --max-instances=$API_INSTANCES_MAX"
    deploy_cmd="$deploy_cmd --cpu=1"
    deploy_cmd="$deploy_cmd --memory=512Mi"
    deploy_cmd="$deploy_cmd --timeout=$API_REQUEST_TIMEOUT"
    # See the API_INSTANCES_MAX comment above (issue #389): 80 let far more
    # requests pile onto this 1-vCPU instance than it could actually run
    # concurrently, so the autoscaler didn't add instances until CPU was
    # already saturated. 20 keeps typical per-instance utilization ~75%,
    # comfortably under the ~25-concurrency point where CPU saturates.
    deploy_cmd="$deploy_cmd --concurrency=20"

    # Tag the service/revision with the git sha so past deployments are
    # identifiable when picking a rollback target (see issue #228).
    deploy_cmd="$deploy_cmd --labels=git-sha=$GIT_SHA"

    # Fold GE_FEED_GENERATOR_DID into this single deploy whenever we can resolve
    # it up front (prod is a constant; stage/dev is derived from the service URL,
    # which is stable once the service exists). This avoids emitting a second,
    # otherwise-identical revision from a post-deploy `services update` — and
    # keeps the git-sha label on the revision that actually serves traffic. Only
    # a brand-new service (first-ever deploy) can't be resolved yet; that case
    # falls back to a follow-up update below. Note the emulator host vars need no
    # explicit removal here: --set-env-vars replaces the whole env set, so any
    # GE_FIRESTORE_EMULATOR_HOST/FIRESTORE_EMULATOR_HOST are dropped by omission.
    local generator_did=""
    local did_in_deploy=false
    if generator_did=$(resolve_generator_did); then
        deploy_cmd="$deploy_cmd --set-env-vars=GE_FEED_GENERATOR_DID=$generator_did"
        did_in_deploy=true
    fi

    # Allow unauthenticated access (adjust based on your needs)
    deploy_cmd="$deploy_cmd --allow-unauthenticated"

    log_build "Executing: $deploy_cmd"
    if ! eval "$deploy_cmd"; then
        log_error "Failed to deploy greenearth-api-$ENVIRONMENT"
        exit 1
    fi

    log_info "✓ greenearth-api-$ENVIRONMENT deployed successfully"

    local service_url
    service_url=$(gcloud run services describe "greenearth-api-$ENVIRONMENT" --region="$REGION" --project="$PROJECT_ID" --format="value(status.url)")
    log_info "Service URL: $service_url"

    if [ "$did_in_deploy" = true ]; then
        log_info "Set GE_FEED_GENERATOR_DID=$generator_did"
    else
        # First-ever deploy: the service URL wasn't known until the service
        # existed, so set the DID now. This creates one extra revision, but only
        # once in a service's lifetime — every later deploy folds the DID in
        # above. Carry the git-sha label onto this revision too.
        local service_host
        service_host=$(echo "$service_url" | sed 's|https://||')
        generator_did="did:web:$service_host"
        gcloud run services update "greenearth-api-$ENVIRONMENT" \
            --region="$REGION" \
            --project="$PROJECT_ID" \
            --labels="git-sha=$GIT_SHA" \
            --update-env-vars="GE_FEED_GENERATOR_DID=$generator_did" \
            --remove-env-vars="GE_FIRESTORE_EMULATOR_HOST,FIRESTORE_EMULATOR_HOST" > /dev/null
        log_info "Set GE_FEED_GENERATOR_DID=$generator_did (follow-up revision)"
    fi

    reset_traffic_to_latest
}

# A rollback (scripts/rollback.sh) pins traffic to a named revision, which takes
# LATEST out of the traffic split — after that, deploying would create a
# perfectly healthy revision that serves nothing. Resetting to LATEST here makes
# "deploy the fix" the way out of a rolled-back state, with no extra step to
# remember. On a normal deploy this is a no-op. It runs only after the deploy
# above succeeded, so a failed build leaves traffic where the rollback put it
# (see issue #181).
reset_traffic_to_latest() {
    log_info "Pointing traffic at the latest revision..."

    if ! gcloud run services update-traffic "greenearth-api-$ENVIRONMENT" \
        --region="$REGION" \
        --project="$PROJECT_ID" \
        --to-latest \
        --quiet > /dev/null; then
        log_error "Deployed successfully, but could not point traffic at the new revision."
        log_error "The previous revision is still serving. Retry with:"
        log_error "  gcloud run services update-traffic greenearth-api-$ENVIRONMENT --region=$REGION --to-latest"
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

_validate_bsky_publisher() {
    local account_label="$1"
    local publisher_id="$2"
    local bsky_secret="$3"

    local bsky_password
    if ! bsky_password=$(gcloud secrets versions access latest \
        --secret="$bsky_secret" --project="$PROJECT_ID" 2>/dev/null); then
        bsky_password=""
    fi
    if [ -z "$bsky_password" ]; then
        log_error "Could not fetch $account_label app password from secret '$bsky_secret'."
        return 1
    fi

    log_info "Validating Bluesky publisher → $account_label ($publisher_id)..."
    if ! pipenv run python scripts/publish_feed.py \
        --handle "$publisher_id" \
        --app-password "$bsky_password" \
        --list > /dev/null; then
        log_error "Could not authenticate the $account_label Bluesky publisher."
        return 1
    fi
}

preflight_bsky_publishers() {
    log_info "Validating Bluesky publishing credentials before deployment..."

    if [ "$ENVIRONMENT" = "prod" ]; then
        if ! _validate_bsky_publisher \
            "GreenEarth" \
            "$PROD_BSKY_PUBLISHER_ID" \
            "$PROD_BSKY_SECRET"; then
            return 1
        fi
        if ! _validate_bsky_publisher \
            "Caterpie" \
            "$CATERPIE_BSKY_PUBLISHER_ID" \
            "$CATERPIE_PROD_BSKY_SECRET"; then
            return 1
        fi
    else
        if ! _validate_bsky_publisher \
            "Caterpie" \
            "$CATERPIE_BSKY_PUBLISHER_ID" \
            "$CATERPIE_STAGE_BSKY_SECRET"; then
            return 1
        fi
    fi

    log_info "Bluesky publishing credentials are valid."
}

prepare_ux_posts() {
    # Content validation is offline and instant, so it always runs.
    if ! pipenv run python scripts/manage_ux_posts.py check; then
        log_error "UX post content is invalid; Cloud Run was not changed."
        exit 1
    fi

    if [ "$SKIP_UX_POST_SYNC" = true ]; then
        log_warn "Skipping UX post sync (--skip-ux-post-sync)."
        log_warn "The revision will ship whatever src/app/ux_posts_resolved.json holds."
        return 0
    fi

    # Resolving needs no credentials: it matches content against the account's public
    # records. Only publishing a genuinely new or edited post needs the app password,
    # so fetch it lazily and fail loudly if something is actually missing.
    log_info "Resolving UX posts against $NOTIFY_BSKY_PUBLISHER_ID..."
    if pipenv run python scripts/manage_ux_posts.py resolve --require-complete; then
        log_info "All UX posts are already published."
        return 0
    fi

    local bsky_password
    if ! bsky_password=$(gcloud secrets versions access latest \
        --secret="$NOTIFY_BSKY_SECRET" --project="$PROJECT_ID" 2>/dev/null); then
        bsky_password=""
    fi
    if [ -z "$bsky_password" ]; then
        log_error "UX posts need publishing but '$NOTIFY_BSKY_SECRET' is unavailable."
        log_error "Create it with scripts/gcp_setup.sh --notify-bsky-app-password ..."
        exit 1
    fi

    log_info "Publishing new or edited UX posts..."
    if ! pipenv run python scripts/manage_ux_posts.py \
        --handle "$NOTIFY_BSKY_PUBLISHER_ID" \
        --app-password "$bsky_password" \
        sync; then
        log_error "UX post sync failed; Cloud Run was not changed."
        exit 1
    fi

    # The manifest must be complete, or the revision would serve placeholders.
    if ! pipenv run python scripts/manage_ux_posts.py resolve --require-complete; then
        log_error "UX posts are still unresolved after syncing; Cloud Run was not changed."
        exit 1
    fi
    log_info "UX posts are ready."
}

load_deployed_video_post_uris() {
    local revisions_json
    if ! revisions_json=$(gcloud run revisions list \
        --service="greenearth-api-$ENVIRONMENT" \
        --region="$REGION" \
        --project="$PROJECT_ID" \
        --limit=20 \
        --format=json 2>/dev/null); then
        return 0
    fi

    local env_values
    if ! env_values=$(pipenv run python -c '
import json
import sys

names = {
    "GE_PINNED_POST_YOUR_FEED_URI",
    "GE_PINNED_POST_YOUR_FEED_EXPLORE_URI",
}
found = {}
for revision in json.load(sys.stdin):
    containers = revision.get("spec", {}).get("containers", [])
    for entry in containers[0].get("env", []) if containers else []:
        name = entry.get("name")
        value = entry.get("value")
        if name in names and name not in found and isinstance(value, str) and value:
            found[name] = value
for name, value in found.items():
    print(f"{name}\t{value}")
' <<< "$revisions_json"); then
        return 0
    fi

    local env_name
    local env_value
    while IFS=$'\t' read -r env_name env_value; do
        case "$env_name" in
            GE_PINNED_POST_YOUR_FEED_URI)
                [ -n "$GE_PINNED_POST_YOUR_FEED_URI" ] \
                    || GE_PINNED_POST_YOUR_FEED_URI="$env_value"
                ;;
            GE_PINNED_POST_YOUR_FEED_EXPLORE_URI)
                [ -n "$GE_PINNED_POST_YOUR_FEED_EXPLORE_URI" ] \
                    || GE_PINNED_POST_YOUR_FEED_EXPLORE_URI="$env_value"
                ;;
        esac
    done <<< "$env_values"
}

prepare_video_post_uris() {
    load_deployed_video_post_uris

    if [ -z "$GE_PINNED_POST_YOUR_FEED_URI" ] \
        || [ -z "$GE_PINNED_POST_YOUR_FEED_EXPLORE_URI" ]; then
        log_error "MySky's two native-video post URIs are required before deployment."
        log_error "Export GE_PINNED_POST_YOUR_FEED_URI and GE_PINNED_POST_YOUR_FEED_EXPLORE_URI."
        exit 1
    fi
    export GE_PINNED_POST_YOUR_FEED_URI GE_PINNED_POST_YOUR_FEED_EXPLORE_URI
}

_sync_feeds_to_account() {
    local account_label="$1"
    local publisher_id="$2"
    local bsky_secret="$3"
    local generator_did="$4"
    local visibility_flag="$5"   # "--public-only", "--internal-only", or ""

    local bsky_password
    if ! bsky_password=$(gcloud secrets versions access latest \
        --secret="$bsky_secret" --project="$PROJECT_ID" 2>/dev/null); then
        bsky_password=""
    fi
    if [ -z "$bsky_password" ]; then
        log_error "Could not fetch $account_label app password from secret '$bsky_secret'."
        return 1
    fi

    log_info "Syncing feeds → $account_label ($publisher_id)..."

    # shellcheck disable=SC2086
    if ! pipenv run python scripts/publish_feed.py \
        --handle "$publisher_id" \
        --app-password "$bsky_password" \
        --generator-did "$generator_did" \
        --environment "$ENVIRONMENT" \
        --sync \
        $visibility_flag; then
        log_error "$account_label feed sync failed."
        return 1
    fi

    log_info "$account_label feed sync complete"
}

sync_feeds() {
    log_info "Syncing feed generator records for $ENVIRONMENT..."

    local generator_did
    if ! generator_did=$(resolve_generator_did); then
        log_error "Could not determine service URL — feed sync cannot continue."
        return 1
    fi

    log_info "Generator DID: $generator_did"

    if [ "$ENVIRONMENT" = "prod" ]; then
        # Prod: two-pass sync
        #   Pass 1 — public feeds → GreenEarth account (original names)
        #   Pass 2 — internal feeds → Caterpie account (obfuscated names)
        if ! _sync_feeds_to_account \
            "GreenEarth" \
            "$PROD_BSKY_PUBLISHER_ID" \
            "$PROD_BSKY_SECRET" \
            "$generator_did" \
            "--public-only"; then
            return 1
        fi

        if ! _sync_feeds_to_account \
            "Caterpie" \
            "$CATERPIE_BSKY_PUBLISHER_ID" \
            "$CATERPIE_PROD_BSKY_SECRET" \
            "$generator_did" \
            "--internal-only"; then
            return 1
        fi
    else
        # Stage/dev: all feeds go to Caterpie (display names get "GE " prefix)
        if ! _sync_feeds_to_account \
            "Caterpie" \
            "$CATERPIE_BSKY_PUBLISHER_ID" \
            "$CATERPIE_STAGE_BSKY_SECRET" \
            "$generator_did" \
            ""; then
            return 1
        fi
    fi
}

main() {
    log_info "Starting Green Earth API deployment..."
    log_info "Project: $PROJECT_ID"
    log_info "Region: $REGION"
    log_info "Environment: $ENVIRONMENT"

    require_clean_worktree
    validate_config
    prepare_video_post_uris
    prepare_ux_posts
    if ! preflight_bsky_publishers; then
        log_error "Bluesky publishing preflight failed; Cloud Run was not changed."
        exit 1
    fi
    verify_vpc_connector

    # Configure kubectl if needed for ES URL auto-detection
    if [ "$GE_ELASTICSEARCH_URL" = "INTERNAL_LB_PLACEHOLDER" ]; then
        configure_kubectl
    fi

    get_elasticsearch_internal_lb_ip
    generate_requirements
    deploy_api_service
    if ! sync_feeds; then
        log_error "Cloud Run deployed, but Bluesky feed sync failed."
        exit 1
    fi

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
        --elasticsearch-url)
            GE_ELASTICSEARCH_URL="$2"
            shift 2
            ;;
        --two-tower-knn-index)
            GE_TWO_TOWER_KNN_INDEX="$2"
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
        --skip-ux-post-sync)
            SKIP_UX_POST_SYNC=true
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --project-id ID          GCP project ID (default: greenearth-471522)"
            echo "  --region REGION          GCP region (default: us-east1)"
            echo "  --environment ENV        Environment name (default: stage)"
            echo "  --elasticsearch-url URL  Elasticsearch URL (default: INTERNAL_LB_PLACEHOLDER)"
            echo "  --two-tower-knn-index IDX  Index for two-tower kNN (default: posts_recent_quality;"
            echo "                             use posts_recent if the quality corpus is not backfilled)"
            echo "  --min-instances N        Minimum instances (default: 1)"
            echo "  --max-instances N        Maximum instances (default: 20)"
            echo "  --timeout SECONDS        Cloud Run request timeout (default: 60)"
            echo "  --skip-ux-post-sync      Ship the current UX post manifest without syncing"
            echo "  --help                   Show this help message"
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
