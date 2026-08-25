#!/bin/bash

# Green Earth API - Cloud Run rollback
#
# Shifts all traffic back to a previously deployed Cloud Run revision. Because
# deploy.sh uses source deploys (`gcloud run deploy --source=.`), the repo never
# names an image tag — each revision is the durable record of a deployment,
# pinning both the built image digest and the env/secret configuration it ran
# with. Rolling back therefore means re-pointing traffic at an older revision,
# not rebuilding an older git sha (see issue #181).
#
# Rollbacks are manual. Cloud Run's own health-check behavior is untouched: a
# revision that never becomes Ready never receives traffic in the first place.
#
# After a rollback, traffic is pinned to a named revision. The next successful
# deploy.sh run resets traffic to LATEST, so "deploy the fix" is also how you
# leave the rolled-back state.
#
# Prerequisites: gcloud authenticated against the project. kubectl access to the
# environment's GKE cluster is optional (used only to warn about a stale
# Elasticsearch address on the target revision).

set -e

# Configuration (overridden by CLI args)
PROJECT_ID="greenearth-471522"
REGION="us-east1"
ENVIRONMENT="stage"

# Rollback target: a revision name (greenearth-api-stage-00287-6nf) or a git sha
# (7176a35). Empty means "the previous deployment", resolved automatically.
TARGET=""

LIST_ONLY=false
DRY_RUN=false
ASSUME_YES=false

# How long to wait for /health to report the rolled-back git sha.
HEALTH_TIMEOUT_SEC=60

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

log_action() {
    echo -e "${BLUE}[ROLLBACK]${NC} $1"
}

service_name() {
    echo "greenearth-api-$ENVIRONMENT"
}

require_service() {
    if ! gcloud run services describe "$(service_name)" \
        --region="$REGION" --project="$PROJECT_ID" > /dev/null 2>&1; then
        log_error "Cloud Run service $(service_name) not found in $REGION."
        log_error "Check --environment (stage/prod), --region, and --project-id."
        exit 1
    fi
}

# Revision currently serving traffic. The deploy script always sends 100% to one
# revision, so a single name is expected here.
serving_revision() {
    gcloud run services describe "$(service_name)" \
        --region="$REGION" \
        --project="$PROJECT_ID" \
        --format="value(status.traffic.revisionName)" | head -n 1
}

revision_git_sha() {
    local revision="$1"
    gcloud run revisions describe "$revision" \
        --region="$REGION" \
        --project="$PROJECT_ID" \
        --format="value(metadata.labels.git-sha)" 2>/dev/null || true
}

revision_env_var() {
    local revision="$1"
    local var_name="$2"
    gcloud run revisions describe "$revision" \
        --region="$REGION" \
        --project="$PROJECT_ID" \
        --flatten="spec.containers[].env[]" \
        --format="value(spec.containers.env.name,spec.containers.env.value)" 2>/dev/null \
        | awk -F'\t' -v name="$var_name" '$1 == name { print $2; exit }'
}

# Ready revisions, newest first, as "name<TAB>git-sha<TAB>created".
ready_revisions() {
    gcloud run revisions list \
        --service="$(service_name)" \
        --region="$REGION" \
        --project="$PROJECT_ID" \
        --filter="status.conditions.type=Ready AND status.conditions.status=True" \
        --sort-by="~metadata.creationTimestamp" \
        --format="value(metadata.name,metadata.labels.git-sha,metadata.creationTimestamp)"
}

list_revisions() {
    local serving
    serving="$(serving_revision)"

    log_info "Rollback candidates for $(service_name) (newest first):"
    echo ""
    printf "    %-34s %-10s %s\n" "REVISION" "GIT SHA" "CREATED"

    while IFS=$'\t' read -r revision sha created; do
        [ -z "$revision" ] && continue
        local marker="  "
        if [ "$revision" = "$serving" ]; then
            marker="=>"
        fi
        printf "%s  %-34s %-10s %s\n" "$marker" "$revision" "${sha:-(unstamped)}" "$created"
    done <<< "$(ready_revisions)"

    echo ""
    echo "  => currently serving traffic"
    echo ""
    echo "  Roll back to the previous deployment:  $0 --environment $ENVIRONMENT"
    echo "  Roll back to a specific target:        $0 --environment $ENVIRONMENT --to <revision|git-sha>"
}

# Newest Ready revision older than the serving one whose git sha differs from
# what is serving now. Skipping same-sha revisions matters because a first-ever
# deploy emits a follow-up revision carrying the same sha (see deploy.sh) —
# rolling back onto that twin would change nothing.
resolve_previous_revision() {
    local serving="$1"
    local serving_sha="$2"
    local seen_serving=false

    while IFS=$'\t' read -r revision sha _created; do
        [ -z "$revision" ] && continue

        if [ "$revision" = "$serving" ]; then
            seen_serving=true
            continue
        fi

        # Only consider revisions older than the one serving traffic. The list is
        # newest-first, so anything before the serving entry is newer.
        [ "$seen_serving" = false ] && continue

        # An unstamped revision (deployed before git-sha labelling) is still a
        # valid target — it just can't be compared by sha.
        if [ -z "$sha" ] || [ -z "$serving_sha" ] || [ "$sha" != "$serving_sha" ]; then
            echo "$revision"
            return 0
        fi
    done <<< "$(ready_revisions)"

    return 1
}

# Accepts a revision name or a git sha; echoes the resolved revision name.
resolve_target_revision() {
    local requested="$1"

    if gcloud run revisions describe "$requested" \
        --region="$REGION" --project="$PROJECT_ID" > /dev/null 2>&1; then
        echo "$requested"
        return 0
    fi

    # Not a revision name — try it as a git sha, taking the newest Ready match.
    local match
    match="$(ready_revisions | awk -F'\t' -v sha="$requested" '$2 == sha { print $1; exit }')"

    if [ -n "$match" ]; then
        echo "$match"
        return 0
    fi

    return 1
}

# The API reaches Elasticsearch over the VPC at an internal load balancer IP that
# deploy.sh bakes into each revision. If that IP has moved since the target
# revision was deployed, rolling back would restore a dead address — redeploying
# the old git sha is the right move instead. Best effort: no kubectl, no check.
check_elasticsearch_address() {
    local revision="$1"

    local revision_url
    revision_url="$(revision_env_var "$revision" GE_ELASTICSEARCH_URL)"
    if [ -z "$revision_url" ]; then
        return 0
    fi

    if ! command -v kubectl &> /dev/null; then
        log_warn "kubectl not available — skipping Elasticsearch address check."
        log_warn "Target revision points at $revision_url; confirm that is still current."
        return 0
    fi

    local lb_ip
    lb_ip=$(kubectl get service greenearth-es-internal-lb \
        -n "greenearth-$ENVIRONMENT" \
        -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")

    if [ -z "$lb_ip" ] || [ "$lb_ip" = "null" ]; then
        log_warn "Could not read the Elasticsearch internal LB IP — skipping address check."
        log_warn "Target revision points at $revision_url; confirm that is still current."
        return 0
    fi

    local current_url="https://$lb_ip:9200"
    if [ "$revision_url" != "$current_url" ]; then
        log_warn "Target revision's Elasticsearch address looks stale:"
        log_warn "  revision: $revision_url"
        log_warn "  current:  $current_url"
        log_warn "Rolling back would restore the old address. If Elasticsearch has moved,"
        log_warn "redeploy the old git sha instead: git checkout <sha> && ./scripts/deploy.sh"
        STALE_ELASTICSEARCH_ADDRESS=true
    fi
}

confirm_rollback() {
    local serving="$1"
    local serving_sha="$2"
    local target="$3"
    local target_sha="$4"

    echo ""
    if [ "$ENVIRONMENT" = "prod" ]; then
        echo -e "${RED}*** PRODUCTION ROLLBACK ***${NC}"
    fi
    echo "  service:  $(service_name)  ($ENVIRONMENT)"
    echo "  serving:  $serving  (${serving_sha:-unstamped})"
    echo "  target:   $target  (${target_sha:-unstamped})"
    echo ""

    if [ "$ASSUME_YES" = true ]; then
        return 0
    fi

    local prompt="Shift 100% of traffic to the target revision? Type 'yes' to confirm: "
    if [ "$STALE_ELASTICSEARCH_ADDRESS" = true ]; then
        prompt="Elasticsearch address may be stale (see warning above). Type 'yes' to roll back anyway: "
    fi

    local reply
    read -r -p "$prompt" reply
    if [ "$reply" != "yes" ]; then
        log_info "Aborted — nothing changed."
        exit 0
    fi
}

shift_traffic() {
    local target="$1"

    local cmd="gcloud run services update-traffic $(service_name)"
    cmd="$cmd --region=$REGION"
    cmd="$cmd --project=$PROJECT_ID"
    cmd="$cmd --to-revisions=$target=100"

    if [ "$DRY_RUN" = true ]; then
        log_info "[dry run] would execute:"
        echo "  $cmd"
        return 0
    fi

    log_action "Executing: $cmd"
    if ! eval "$cmd" > /dev/null; then
        log_error "Failed to shift traffic to $target"
        exit 1
    fi

    log_info "✓ Traffic now served by $target"
}

# Confirms the rollback took effect from outside Cloud Run's own bookkeeping, by
# asking the running service which git sha it is (see issue #228).
verify_health() {
    local target_sha="$1"

    if [ -z "$target_sha" ]; then
        log_warn "Target revision has no git-sha label — skipping /health verification."
        return 0
    fi

    local service_url
    service_url=$(gcloud run services describe "$(service_name)" \
        --region="$REGION" --project="$PROJECT_ID" --format="value(status.url)")

    log_info "Verifying $service_url/health reports git sha $target_sha..."

    local deadline=$((SECONDS + HEALTH_TIMEOUT_SEC))
    while [ "$SECONDS" -lt "$deadline" ]; do
        local reported
        reported=$(curl -fsS --max-time 10 "$service_url/health" 2>/dev/null \
            | sed -n 's/.*"git_sha"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')

        if [ "$reported" = "$target_sha" ]; then
            log_info "✓ /health reports git sha $reported"
            return 0
        fi

        sleep 3
    done

    log_warn "/health did not report git sha $target_sha within ${HEALTH_TIMEOUT_SEC}s."
    log_warn "Traffic was shifted; check the service manually before assuming the rollback failed."
    return 0
}

report_side_effects() {
    echo ""
    log_info "Rollback complete. Two deploy side effects are NOT rolled back:"
    echo "  - Pinned posts and feed generator records published by the newer deploy stay"
    echo "    live. This is deliberate: old records are retained so rolled-back revisions"
    echo "    keep working (see README, 'Managed pinned posts')."
    echo "  - Debug feed display names still show the newer git sha until the next deploy."
    echo ""
    log_info "To leave the rolled-back state, deploy the fix normally:"
    echo "  ./scripts/deploy.sh --environment $ENVIRONMENT"
    echo "  (deploy.sh resets traffic to LATEST on success)"
}

main() {
    log_info "Green Earth API rollback"
    log_info "Project: $PROJECT_ID"
    log_info "Region: $REGION"
    log_info "Environment: $ENVIRONMENT"

    require_service

    if [ "$LIST_ONLY" = true ]; then
        list_revisions
        exit 0
    fi

    local serving serving_sha target target_sha
    serving="$(serving_revision)"
    if [ -z "$serving" ]; then
        log_error "Could not determine which revision is serving traffic."
        exit 1
    fi
    serving_sha="$(revision_git_sha "$serving")"

    if [ -n "$TARGET" ]; then
        if ! target="$(resolve_target_revision "$TARGET")"; then
            log_error "No revision matches '$TARGET' (tried revision name, then git sha)."
            log_error "Run '$0 --environment $ENVIRONMENT --list' to see candidates."
            exit 1
        fi
    else
        if ! target="$(resolve_previous_revision "$serving" "$serving_sha")"; then
            log_error "No previous Ready revision found to roll back to."
            log_error "Run '$0 --environment $ENVIRONMENT --list' to see what exists."
            exit 1
        fi
    fi

    if [ "$target" = "$serving" ]; then
        log_info "$target is already serving traffic — nothing to do."
        exit 0
    fi

    target_sha="$(revision_git_sha "$target")"

    check_elasticsearch_address "$target"
    confirm_rollback "$serving" "$serving_sha" "$target" "$target_sha"
    shift_traffic "$target"

    if [ "$DRY_RUN" = true ]; then
        log_info "[dry run] no changes made."
        exit 0
    fi

    verify_health "$target_sha"
    report_side_effects
}

STALE_ELASTICSEARCH_ADDRESS=false

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
        --to)
            TARGET="$2"
            shift 2
            ;;
        --list)
            LIST_ONLY=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --yes)
            ASSUME_YES=true
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Rolls the API back to a previously deployed Cloud Run revision by"
            echo "shifting 100% of traffic to it. With no --to, the previous deployment"
            echo "(newest Ready revision older than the one serving, with a different"
            echo "git sha) is used."
            echo ""
            echo "Options:"
            echo "  --environment ENV        Environment name (default: stage)"
            echo "  --to REVISION|GIT_SHA    Roll back to a specific revision or git sha"
            echo "  --list                   List rollback candidates and exit"
            echo "  --dry-run                Show what would change, execute nothing"
            echo "  --yes                    Skip the confirmation prompt"
            echo "  --project-id ID          GCP project ID (default: greenearth-471522)"
            echo "  --region REGION          GCP region (default: us-east1)"
            echo "  --help                   Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0 --environment prod --list"
            echo "  $0 --environment prod"
            echo "  $0 --environment prod --to 7176a35"
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
