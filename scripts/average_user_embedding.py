#!/usr/bin/env python3
"""Average two-tower embeddings for active feed users and save a local artifact.

Run ``pipenv run python scripts/average_user_embedding.py --help`` for configuration. Credentials
are read only from POSTHOG_PERSONAL_API_KEY, GE_ELASTICSEARCH_API_KEY, and
GE_API_KEY. The embedding API must read the same Elasticsearch environment.
"""

# Flow: PostHog activity cohort -> retained ES like counts -> actual user embeddings
# -> normalized mean -> local JSON. Promotion is a separate command so a person can
# inspect this artifact before selecting it for an environment.

import argparse
import json
import logging
import math
import os
import re
import sys
import tempfile
import time
import uuid
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from app.lib.average_user_embedding_artifact import (  # noqa: E402
    ArtifactValidationError,
    is_count,
    is_finite_number,
    model_id,
    validate_artifact,
    validate_history_policy,
)

logger = logging.getLogger(__name__)

LIKES_INDEX = "likes"
POSTHOG_PAGE_SIZE = 1000
ES_BATCH_SIZE = 500
REQUEST_TIMEOUT_SECONDS = 60
PROGRESS_LOG_INTERVAL_SECONDS = 10


class RunError(Exception):
    """A collection or configuration error that prevents saving an average."""


def post_json(client: httpx.Client, path: str, payload: dict) -> dict:
    """Send one request; callers handle failures and validate the response contents."""
    response = client.post(path, json=payload)
    response.raise_for_status()
    try:
        data = response.json()
    except ValueError:
        raise RunError(f"Invalid JSON response from {response.url}") from None
    if not isinstance(data, dict):
        raise RunError(f"Expected a JSON object from {response.url}")
    return data


def collect_posthog_users(client, project_id, minimum, cutoff):
    """Keyset-page complete user counts, including when responses cap rows early."""
    # Every page shares the same upper event timestamp. Advancing by DID rather
    # than OFFSET keeps each user's complete count together; values are bound
    # parameters, not interpolated SQL. There is no lower date bound or feed filter.
    query = """
        SELECT distinct_id, count() AS interaction_seen_count
        FROM events
        WHERE event = 'interactionSeen'
          AND timestamp < parseDateTimeBestEffort({cutoff})
          AND distinct_id > {after_did}
        GROUP BY distinct_id
        HAVING count() >= {minimum}
        ORDER BY distinct_id ASC
        LIMIT {page_size}
    """
    users = {}
    cursor = ""
    page = 0
    logger.info(
        "PostHog: selecting interactionSeen users across all history/feeds; "
        "project=%s minimum=%d cutoff=%s",
        project_id,
        minimum,
        cutoff,
    )
    while True:
        page += 1
        started = time.monotonic()
        logger.info("PostHog: requesting page %d (limit=%d)", page, POSTHOG_PAGE_SIZE)
        result = post_json(
            client,
            f"/api/projects/{project_id}/query/",
            {
                "query": {
                    "kind": "HogQLQuery",
                    "query": query,
                    "values": {
                        "cutoff": cutoff,
                        "after_did": cursor,
                        "minimum": minimum,
                        "page_size": POSTHOG_PAGE_SIZE,
                    },
                },
                "refresh": "force_blocking",
            },
        )
        rows = result.get("results")
        query_status = result.get("query_status")
        if (
            not isinstance(rows, list)
            or result.get("error")
            or (
                isinstance(query_status, dict)
                and (query_status.get("error") or query_status.get("complete") is False)
            )
        ):
            raise RunError("PostHog: incomplete or invalid query response")
        # A short page is not proof of exhaustion: PostHog may cap results below
        # our requested limit. Continue until a query returns no rows.
        if not rows:
            logger.info(
                "PostHog: page %d empty; pagination complete in %.2fs; %d qualifying users",
                page,
                time.monotonic() - started,
                len(users),
            )
            return users
        for row in rows:
            if not isinstance(row, list) or len(row) != 2:
                raise RunError("PostHog: invalid user row")
            did, count = row
            if not isinstance(did, str) or not re.fullmatch(
                r"did:[a-z0-9]+:[A-Za-z0-9._:%-]+(?::[A-Za-z0-9._:%-]+)*", did
            ):
                raise RunError("PostHog: distinct_id is not a valid DID")
            if not is_count(count) or count < minimum:
                raise RunError("PostHog: invalid interaction count")
            users[did] = count
        # GROUP BY gives one row per DID; ORDER BY puts the next cursor last.
        next_cursor = rows[-1][0]
        if next_cursor <= cursor:
            raise RunError("PostHog: pagination did not advance")
        cursor = next_cursor
        logger.info(
            "PostHog: page %d received %d users in %.2fs; total=%d",
            page,
            len(rows),
            time.monotonic() - started,
            len(users),
        )


def collect_like_counts(client, dids):
    """Count exact retained likes; every requested author fits in every shard."""
    # These are counts in the retained likes index, not lifetime likes or the
    # smaller usable history window later loaded by the API. Missing terms stay zero.
    counts = dict.fromkeys(dids, 0)
    ordered = sorted(counts)
    batch_count = (len(ordered) + ES_BATCH_SIZE - 1) // ES_BATCH_SIZE
    logger.info(
        "Elasticsearch: counting retained likes for %d DIDs in %d batches; index=%s",
        len(ordered),
        batch_count,
        LIKES_INDEX,
    )
    for start in range(0, len(ordered), ES_BATCH_SIZE):
        batch = ordered[start : start + ES_BATCH_SIZE]
        batch_number = start // ES_BATCH_SIZE + 1
        started = time.monotonic()
        logger.info(
            "Elasticsearch: requesting batch %d/%d (%d DIDs)", batch_number, batch_count, len(batch)
        )
        # Filtering to this batch bounds the number of possible author buckets.
        # Both size limits can therefore include every term, even on each shard.
        result = post_json(
            client,
            f"/{LIKES_INDEX}/_search",
            {
                "size": 0,
                "query": {"terms": {"author_did": batch}},
                "aggs": {
                    "users": {
                        "terms": {
                            "field": "author_did",
                            "size": len(batch),
                            "shard_size": len(batch),
                            "show_term_doc_count_error": True,
                        }
                    }
                },
            },
        )
        shards = result.get("_shards")
        aggregations = result.get("aggregations")
        if not isinstance(shards, dict) or not isinstance(aggregations, dict):
            raise RunError("Elasticsearch: invalid aggregation metadata")
        aggregation = aggregations.get("users")
        if not isinstance(aggregation, dict):
            raise RunError("Elasticsearch: invalid user aggregation")
        # Approximate or partial counts could wrongly exclude users at the cutoff.
        # Require a complete, exact aggregation before applying the like threshold.
        if (
            result.get("timed_out") is not False
            or type(shards.get("failed")) is not int
            or shards.get("failed") != 0
            or not is_count(shards.get("total"))
            or not is_count(shards.get("successful"))
            or shards.get("successful") != shards.get("total")
            or result.get("terminated_early", False) is not False
            or type(aggregation.get("sum_other_doc_count")) is not int
            or aggregation.get("sum_other_doc_count") != 0
            or type(aggregation.get("doc_count_error_upper_bound")) is not int
            or aggregation.get("doc_count_error_upper_bound") != 0
        ):
            raise RunError("Elasticsearch: incomplete or inexact like counts")
        buckets = aggregation.get("buckets")
        if not isinstance(buckets, list):
            raise RunError("Elasticsearch: missing like-count buckets")
        seen = set()
        for bucket in buckets:
            if not isinstance(bucket, dict):
                raise RunError("Elasticsearch: invalid like-count bucket")
            did, count = bucket.get("key"), bucket.get("doc_count")
            if (
                not isinstance(did, str)
                or did not in batch
                or did in seen
                or not is_count(count)
                or type(bucket.get("doc_count_error_upper_bound", 0)) is not int
                or bucket.get("doc_count_error_upper_bound", 0) != 0
            ):
                raise RunError("Elasticsearch: invalid or inexact like-count bucket")
            seen.add(did)
            counts[did] = count
        logger.info(
            "Elasticsearch: batch %d/%d complete in %.2fs; %d buckets, "
            "%d missing DIDs counted as zero",
            batch_number,
            batch_count,
            time.monotonic() - started,
            len(seen),
            len(batch) - len(seen),
        )
    return counts


def fetch_embedding(client, did):
    """Validate one response and return only what aggregation needs."""
    result = post_json(client, "/embeddings/user", {"user_did": did})
    if result.get("user_did") != did:
        raise RunError("Embedding API: response DID differs from request")
    if result.get("likes_index") != LIKES_INDEX:
        raise RunError("Embedding API: Elasticsearch source mismatch (likes_index)")
    policy = validate_history_policy(result.get("history_policy"))
    for key in ("history_like_count", "history_embedding_count"):
        if not is_count(result.get(key)):
            raise RunError(f"Embedding API: {key} must be a nonnegative integer")
    likes, usable = result["history_like_count"], result["history_embedding_count"]
    if usable > likes or likes > policy["limit"]:
        raise RunError("Embedding API: history counts exceed their limits")
    if result.get("status") == "skipped":
        # Missing usable history is a legitimate exclusion, not a transport
        # failure. It must not carry an empty-history substitute embedding.
        reason = result.get("reason")
        if (
            reason not in ("no_likes", "no_embedded_history")
            or any(
                result.get(key) is not None
                for key in ("embedding", "user_model_uuid", "post_model_uuid", "dimension")
            )
            or usable != 0
            or (reason == "no_likes") != (likes == 0)
        ):
            raise RunError("Embedding API: skip reason/counts/vector inconsistent")
        return {"status": "skipped", "reason": reason, "history_policy": policy}
    vector, dimension = result.get("embedding"), result.get("dimension")
    if (
        result.get("status") != "ok"
        or not isinstance(vector, list)
        or not vector
        or not all(is_finite_number(value) for value in vector)
        or not is_count(dimension)
        or dimension != len(vector)
        or usable == 0
    ):
        raise RunError(
            "Embedding API: expected finite nonempty vector, matching dimension and usable history"
        )
    return {
        "status": "ok",
        "history_policy": policy,
        "embedding": vector,
        "dimension": dimension,
        "user_model_uuid": model_id(result.get("user_model_uuid")),
        "post_model_uuid": model_id(result.get("post_model_uuid")),
    }


def average_embeddings(client, dids):
    """L2-normalize the equal-weight mean; any failed request stops the run."""
    started = time.monotonic()
    next_progress_at = started + PROGRESS_LOG_INTERVAL_SECONDS
    skipped = Counter()
    vectors = []
    model_pair = dimension = policy = None
    logger.info(
        "Embeddings: requesting %d users sequentially; timeout=%ds",
        len(dids),
        REQUEST_TIMEOUT_SECONDS,
    )
    for did in dids:
        result = fetch_embedding(client, did)
        # All contributors must use the same history policy and model pair.
        current_policy = result["history_policy"]
        if policy is not None and current_policy != policy:
            raise RunError("Embedding API: mixed history policies")
        policy = current_policy
        if result["status"] == "skipped":
            skipped[result["reason"]] += 1
        else:
            current_pair = (result["user_model_uuid"], result["post_model_uuid"])
            current_dimension = result["dimension"]
            if model_pair is not None and (
                current_pair != model_pair or current_dimension != dimension
            ):
                raise RunError("Embedding API: mixed model pairs or vector dimensions")
            model_pair, dimension = current_pair, current_dimension
            vectors.append(result["embedding"])
        now = time.monotonic()
        if now >= next_progress_at:
            logger.info(
                "Embeddings: progress %d/%d completed contributing=%d skipped=%d elapsed=%.2fs",
                len(vectors) + skipped.total(),
                len(dids),
                len(vectors),
                skipped.total(),
                now - started,
            )
            next_progress_at = now + PROGRESS_LOG_INTERVAL_SECONDS
    logger.info(
        "Embeddings: summary eligible=%d contributing=%d skipped=%d",
        len(dids),
        len(vectors),
        skipped.total(),
    )
    for reason, count in sorted(skipped.items()):
        logger.warning("Embeddings: skipped reason=%s count=%d", reason, count)
    if not vectors or model_pair is None:
        raise RunError("No valid user embeddings; no average was written")
    # Each user gets one vote: activity and like counts selected the cohort but
    # do not weight the mean. fsum reduces cancellation/rounding error per coordinate.
    try:
        mean = [math.fsum(column) / len(vectors) for column in zip(*vectors, strict=True)]
    except (OverflowError, ValueError):
        raise RunError("The average is not finite; no average was written") from None
    magnitude = math.hypot(*mean)
    if (
        not all(is_finite_number(value) for value in mean)
        or not math.isfinite(magnitude)
        or magnitude == 0
    ):
        raise RunError("The average must be finite and nonzero; no average was written")
    # Normalize only after averaging. Normalizing inputs here would change the
    # mean's direction when their magnitudes differ; a zero mean has no direction.
    mean = [value / magnitude for value in mean]
    logger.info(
        "Average: computed L2-normalized unweighted mean contributors=%d dimension=%d "
        "user_model_uuid=%s post_model_uuid=%s",
        len(vectors),
        dimension,
        model_pair[0],
        model_pair[1],
    )
    return {
        "embedding": mean,
        "dimension": dimension,
        "user_model_uuid": model_pair[0],
        "post_model_uuid": model_pair[1],
        "history_policy": policy,
        "contributing_users": len(vectors),
        "skipped_users": skipped.total(),
    }


def utc_string(value):
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def atomic_json(destination, value):
    temporary = None
    try:
        # Use the destination directory so the final rename stays on one filesystem.
        # Readers see either the finished JSON or no new artifact, never half a write.
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=destination.parent,
            prefix=".average_user_embedding_",
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            json.dump(value, handle, indent=2, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return destination


def nonnegative_int(value):
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a nonnegative integer") from None
    if number < 0:
        raise argparse.ArgumentTypeError("must be a nonnegative integer")
    return number


def positive_int(value):
    number = nonnegative_int(value)
    if number == 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return number


def base_url(value):
    # Service URLs are logged, so credentials belong in headers rather than URLs.
    try:
        parts = urlsplit(value)
        _ = parts.port
    except ValueError:
        raise argparse.ArgumentTypeError("must be an HTTP(S) base URL") from None
    if (
        parts.scheme not in ("http", "https")
        or not parts.hostname
        or parts.username is not None
        or parts.password is not None
        or parts.query
        or parts.fragment
    ):
        raise argparse.ArgumentTypeError(
            "use an HTTP(S) base URL without credentials, query, or fragment"
        )
    return value.rstrip("/")


def build_parser():
    # Operational defaults live here; this command does not configure model training.
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--posthog-project-id", type=positive_int, default=509275)
    parser.add_argument("--posthog-host", type=base_url, default="https://us.posthog.com")
    parser.add_argument("--min-interaction-seen", type=nonnegative_int, default=50)
    parser.add_argument("--es-url", type=base_url, default="https://localhost:9200")
    parser.add_argument("--min-likes", type=nonnegative_int, default=5)
    parser.add_argument("--api-url", type=base_url, default="http://localhost:8300")
    parser.add_argument(
        "--es-insecure",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="skip Elasticsearch TLS certificate verification (default: %(default)s)",
    )
    parser.add_argument("--output-dir", default="./outputs/average_user_embeddings/")
    return parser


def generate(args, run_id, started_at):
    credentials = {}
    for name in ("POSTHOG_PERSONAL_API_KEY", "GE_ELASTICSEARCH_API_KEY", "GE_API_KEY"):
        value = os.environ.get(name, "").strip()
        if not value or any(character.isspace() for character in value):
            raise RunError(f"Set {name} to a valid API key")
        credentials[name] = value
    # Freeze the PostHog cutoff once, rounded down to whole seconds, for every page.
    cutoff = utc_string(started_at.replace(microsecond=0))
    # Reuse one connection pool per service. Only ES may skip TLS verification.
    with (
        httpx.Client(
            base_url=args.posthog_host,
            headers={"Authorization": f"Bearer {credentials['POSTHOG_PERSONAL_API_KEY']}"},
            timeout=REQUEST_TIMEOUT_SECONDS,
            follow_redirects=False,
        ) as posthog,
        httpx.Client(
            base_url=args.es_url,
            headers={"Authorization": f"ApiKey {credentials['GE_ELASTICSEARCH_API_KEY']}"},
            verify=not args.es_insecure,
            timeout=REQUEST_TIMEOUT_SECONDS,
            follow_redirects=False,
        ) as es,
        httpx.Client(
            base_url=args.api_url,
            headers={"X-API-Key": credentials["GE_API_KEY"]},
            timeout=REQUEST_TIMEOUT_SECONDS,
            follow_redirects=False,
        ) as api,
    ):
        # Get our (MySky) users with a minimum number of interactionSeen events
        users = collect_posthog_users(
            posthog, args.posthog_project_id, args.min_interaction_seen, cutoff
        )
        # Filter users that have at least min_likes likes
        likes = collect_like_counts(es, users)
        eligible = [did for did in sorted(users) if likes[did] >= args.min_likes]
        logger.info(
            "Elasticsearch: %d users meet like threshold >=%d; %d filtered out",
            len(eligible),
            args.min_likes,
            len(users) - len(eligible),
        )
        # Get the user tower embeddings for each user and average the result
        result = average_embeddings(api, eligible)
    skipped_users = result.pop("skipped_users")
    # The consumer receives one mean plus provenance/coverage, never individual
    # DIDs, activity records, credentials, or individual user vectors.
    artifact = {
        "format_version": 1,
        "run_id": run_id,
        "source_completed_at": utc_string(datetime.now(UTC)),
        **result,
        "cohort": {
            "cutoff": cutoff,
            "min_interaction_seen": args.min_interaction_seen,
            "min_likes": args.min_likes,
            "posthog_users": len(users),
            "below_min_likes": len(users) - len(eligible),
            "eligible_users": len(eligible),
            "skipped_users": skipped_users,
        },
    }
    validate_artifact(artifact)
    return artifact


def run(args):
    """Generate one local artifact and return a small status summary."""
    clock_started = time.monotonic()
    summary = {
        "status": "failed",
        "artifact_path": None,
    }
    try:
        started = datetime.now(UTC)
        # The timestamp makes runs recognizable; the random suffix distinguishes
        # separate runs even if their clocks produce the same timestamp.
        run_id = started.strftime("%Y%m%dT%H%M%S.%fZ_") + uuid.uuid4().hex[:8]
        directory = Path(args.output_dir).expanduser().resolve()
        logger.info("Run: started run_id=%s output_dir=%s", run_id, directory)
        directory.mkdir(parents=True, exist_ok=True)
        artifact = generate(args, run_id, started)
        # All collection and contract checks finish before the artifact is written.
        artifact_path = directory / f"average_user_embedding_{run_id}.json"
        atomic_json(artifact_path, artifact)
        summary["artifact_path"] = str(artifact_path)
        summary["run_id"] = artifact["run_id"]
        logger.info("Artifact: %s", artifact_path)
        summary["status"] = "success"
    except (RunError, ArtifactValidationError, httpx.HTTPError) as error:
        reason = str(error) or type(error).__name__
        if isinstance(error, httpx.RequestError):
            reason = (
                f"{type(error).__name__} for {error.request.method} {error.request.url}: {reason}"
            )
        summary["error"] = reason
        logger.error("Run: failed: %s", reason)
    except KeyboardInterrupt:
        summary["error"] = "Run interrupted"
        logger.error("Run: interrupted")
    except Exception as error:
        reason = (
            f"Run failed ({type(error).__name__}); inspect configuration and output permissions"
        )
        summary["error"] = reason
        logger.error("Run: %s", reason)
    finally:
        logger.info("Run: %s in %.2fs", summary["status"], time.monotonic() - clock_started)
    return summary


def main(argv=None):
    args = build_parser().parse_args(argv)
    # Human-readable progress goes to stderr. Keep stdout as one JSON result so
    # callers can capture it without parsing log lines, including on failed runs.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    # HTTPX logs every request at INFO; keep this CLI's output at the stage level.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    summary = run(args)
    print(json.dumps(summary, allow_nan=False))
    return 0 if summary["status"] == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
