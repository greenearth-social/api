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
import ssl
import sys
import tempfile
import time
import uuid
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from http.client import HTTPException
from pathlib import Path
from threading import Lock
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener

# Add the repo's src/ directory so the script can import app.* from any working directory.
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

REASON_HINTS = {
    "no_likes": "No recent likes were loaded; compare the API and script ES environment.",
    "no_embedded_history": "No usable history embeddings; inspect indexed post/reply embeddings.",
    "upstream_error": "History or inference failed; inspect the API container logs.",
    "upstream_timeout": "History or inference timed out; inspect API/inference logs.",
    "invalid_inference_response": "The API rejected inference output or model metadata.",
    "invalid_embedding_response": "The embedding response was invalid; inspect API/inference logs.",
    "network_error": "Check the API URL, container port, and connectivity.",
}


class RunError(Exception):
    """A collection or configuration error that prevents saving an average."""


class RequestError(Exception):
    """A safe, body-free upstream failure."""

    def __init__(self, reason, detail=None):
        # Only controlled reason/detail strings travel through logs and summaries;
        # raw response bodies can contain credentials or user data.
        self.reason = reason
        self.detail = detail
        super().__init__(reason)


class NoRedirect(HTTPRedirectHandler):
    """Do not forward credentials to a redirect target."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def retry_delay(value, attempt):
    """Honor Retry-After seconds or an HTTP date; otherwise back off 1, 2s."""
    if value:
        try:
            delay = float(value)
            if math.isfinite(delay) and delay >= 0:
                return delay
        except ValueError:
            try:
                date = parsedate_to_datetime(value)
                return max(0.0, date.timestamp() - time.time())
            except (TypeError, ValueError, OverflowError):
                pass
    return float(2**attempt)


class JsonClient:
    """Log collection requests; summarize embedding requests at the stage level."""

    def __init__(self, service, base_url, headers, insecure=False):
        self.service = service
        self.base_url = base_url.rstrip("/")
        self.headers = {"Content-Type": "application/json", **headers}
        self._retry_count = 0
        # Embedding workers share this client, including its aggregate retry counter.
        self._retry_lock = Lock()
        context = ssl.create_default_context()
        if insecure:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        self.opener = build_opener(NoRedirect(), HTTPSHandler(context=context))

    @property
    def retry_count(self):
        with self._retry_lock:
            return self._retry_count

    def _log_request(self, level, message, *args):
        # Per-user embedding requests are intentionally quiet. Their outcomes are
        # counted by average_embeddings and reported in its final reason summary.
        if self.service != "Embedding API":
            logger.log(level, message, *args)

    def get(self, path):
        return self.request("GET", path)

    def post(self, path, payload):
        return self.request("POST", path, payload)

    def request(self, method, path, payload=None):
        # Keep one correlation ID across retries of the same logical request.
        request_id = uuid.uuid4().hex
        context = f"{self.service}: {method} {self.base_url}{path} request_id={request_id}"
        request = Request(
            self.base_url + path,
            data=json.dumps(payload, allow_nan=False).encode("utf-8")
            if payload is not None
            else None,
            headers={**self.headers, "X-Request-ID": request_id},
            method=method,
        )
        for attempt in range(3):
            delay = float(2**attempt)
            started = time.monotonic()
            self._log_request(logging.INFO, "%s attempt %d/3 (timeout=60s)", context, attempt + 1)
            try:
                with self.opener.open(request, timeout=60) as response:
                    status = getattr(response, "status", 200)
                    try:
                        data = json.load(response)
                    except (ValueError, UnicodeError):
                        raise RequestError("invalid_json", "response is not valid JSON") from None
                if not isinstance(data, dict):
                    raise RequestError("invalid_json_object", "response must be a JSON object")
                self._log_request(
                    logging.INFO,
                    "%s attempt %d/3 succeeded: HTTP %s in %.2fs",
                    context,
                    attempt + 1,
                    status,
                    time.monotonic() - started,
                )
                return data
            except RequestError as error:
                self._log_request(
                    logging.ERROR,
                    "%s attempt %d/3 rejected after %.2fs: %s (%s)",
                    context,
                    attempt + 1,
                    time.monotonic() - started,
                    error.reason,
                    error.detail,
                )
                raise
            except HTTPError as error:
                status = error.code
                delay = retry_delay(error.headers.get("Retry-After"), attempt)
                # Read only known machine codes; never copy upstream text into output.
                code = None
                try:
                    body = json.loads(error.read(65536))
                    if isinstance(body, dict) and isinstance(body.get("detail"), dict):
                        code = body["detail"].get("code")
                except (ValueError, UnicodeError, OSError, HTTPException):
                    pass
                error.close()
                # These failures require operator action. Raising RunError stops
                # new user submissions, whereas RequestError records one failed user.
                if code in (
                    "inference_not_configured",
                    "upstream_authentication_error",
                    "upstream_configuration_error",
                    "model_metadata_missing",
                ):
                    self._log_request(
                        logging.ERROR, "%s HTTP %d: %s; aborting", context, status, code
                    )
                    raise RunError(f"Embedding API: configuration failure ({code})") from None
                if status in (400, 401, 403, 404, 405, 422) or 300 <= status < 400:
                    self._log_request(
                        logging.ERROR,
                        "%s HTTP %d: authentication/configuration error; aborting",
                        context,
                        status,
                    )
                    raise RunError(
                        f"{self.service}: HTTP {status}; check credentials and configuration"
                    ) from None
                reason = (
                    code
                    if code
                    in (
                        "invalid_inference_response",
                        "upstream_error",
                        "upstream_timeout",
                    )
                    else f"http_{status}"
                )
                detail = f"HTTP {status}; code={reason}"
                if status not in (408, 429) and not 500 <= status < 600:
                    self._log_request(logging.ERROR, "%s %s; not retryable", context, detail)
                    raise RequestError(reason, detail) from None
            except (ssl.SSLError, ssl.CertificateError):
                self._log_request(logging.ERROR, "%s TLS configuration error; aborting", context)
                raise RunError(f"{self.service}: TLS configuration error") from None
            except URLError as error:
                if isinstance(error.reason, ssl.SSLError):
                    self._log_request(
                        logging.ERROR, "%s TLS configuration error; aborting", context
                    )
                    raise RunError(f"{self.service}: TLS configuration error") from None
                reason = "network_error"
                # Exception text may contain URLs/credentials; expose only its type.
                detail = f"transport={type(error.reason).__name__}"
            except (TimeoutError, ConnectionError, OSError, HTTPException) as error:
                reason = "network_error"
                detail = f"transport={type(error).__name__}"
            if delay > 60:
                # Do not silently retry earlier than the server requested, or leave
                # a manual run sleeping indefinitely on a very long Retry-After.
                raise RequestError(
                    "retry_after_too_long", "Retry-After exceeds the 60-second retry budget"
                )
            if attempt == 2:
                self._log_request(
                    logging.ERROR,
                    "%s attempt %d/3 failed in %.2fs: %s; exhausted all 3 attempts",
                    context,
                    attempt + 1,
                    time.monotonic() - started,
                    detail,
                )
                raise RequestError(reason, detail)
            self._log_request(
                logging.WARNING,
                "%s attempt %d/3 failed in %.2fs: %s; retrying in %.1fs",
                context,
                attempt + 1,
                time.monotonic() - started,
                detail,
                delay,
            )
            with self._retry_lock:
                # Count scheduled retries, including workers that have not finished yet.
                self._retry_count += 1
            time.sleep(delay)
        raise AssertionError("unreachable")


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
        LIMIT 1000
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
        logger.info("PostHog: requesting page %d (limit=1000)", page)
        result = client.post(
            f"/api/projects/{project_id}/query/",
            {
                "query": {
                    "kind": "HogQLQuery",
                    "query": query,
                    "values": {"cutoff": cutoff, "after_did": cursor, "minimum": minimum},
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
        previous_count = len(users)
        next_cursor = cursor
        previous = ""
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
            if did < previous:
                raise RunError("PostHog: results are not ordered by DID")
            # A repeated row contains the same aggregate, not another batch of
            # interactions to add. Disagreement makes the cohort unreliable.
            if did in users and users[did] != count:
                raise RunError("PostHog: conflicting duplicate user counts")
            users[did] = count
            previous = did
            next_cursor = max(next_cursor, did)
        if next_cursor <= cursor:
            raise RunError("PostHog: pagination did not advance")
        cursor = next_cursor
        logger.info(
            "PostHog: page %d received %d rows, %d new DIDs, %d duplicates in %.2fs; total=%d",
            page,
            len(rows),
            len(users) - previous_count,
            len(rows) - (len(users) - previous_count),
            time.monotonic() - started,
            len(users),
        )


def collect_like_counts(client, index, dids):
    """Count exact retained likes; every requested author fits in every shard."""
    # These are counts in the retained likes index, not lifetime likes or the
    # smaller usable history window later loaded by the API. Missing terms stay zero.
    counts = dict.fromkeys(dids, 0)
    ordered = sorted(counts)
    batch_count = (len(ordered) + 499) // 500
    logger.info(
        "Elasticsearch: counting retained likes for %d DIDs in %d batches; index=%s",
        len(ordered),
        batch_count,
        index,
    )
    for start in range(0, len(ordered), 500):
        batch = ordered[start : start + 500]
        batch_number = start // 500 + 1
        started = time.monotonic()
        logger.info(
            "Elasticsearch: requesting batch %d/%d (%d DIDs)", batch_number, batch_count, len(batch)
        )
        # Filtering to this batch bounds the number of possible author buckets.
        # Both size limits can therefore include every term, even on each shard.
        result = client.post(
            f"/{quote(index, safe='')}/_search",
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


def fetch_embedding(client, did, expected_source):
    """Validate one response and return only what aggregation needs."""
    try:
        result = client.post("/embeddings/user", {"user_did": did})
        if result.get("user_did") != did:
            raise RequestError("mismatched_user_did", "response DID differs from request")
        # A local ES tunnel and a remote API can accidentally target different
        # environments. Compare data-source identity before trusting any vector.
        for key, value in expected_source.items():
            if result.get(key) != value:
                raise RunError(f"Embedding API: Elasticsearch source mismatch ({key})")
        policy = validate_history_policy(result.get("history_policy"))
        for key in ("history_like_count", "history_embedding_count"):
            if not is_count(result.get(key)):
                raise RequestError("invalid_history_counts", f"{key} must be a nonnegative integer")
        likes, usable = result["history_like_count"], result["history_embedding_count"]
        if usable > likes or likes > policy["limit"]:
            raise RequestError("invalid_history_counts", "history counts exceed their limits")
        if result.get("status") == "skipped":
            # Missing usable history is a legitimate exclusion, not a transport
            # failure. It must not carry an empty-history substitute embedding.
            reason = result.get("reason")
            if (
                reason not in ("no_likes", "no_embedded_history")
                or any(
                    result.get(key) is not None
                    for key in (
                        "embedding",
                        "user_model_uuid",
                        "post_model_uuid",
                        "dimension",
                    )
                )
                or usable != 0
                or (reason == "no_likes") != (likes == 0)
            ):
                raise RequestError(
                    "invalid_skip_response", "skip reason/counts/vector inconsistent"
                )
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
            raise RequestError(
                "invalid_embedding_response",
                "expected finite nonempty vector, matching dimension and usable history",
            )
        user_model = model_id(result.get("user_model_uuid"))
        post_model = model_id(result.get("post_model_uuid"))
        return {
            "status": "ok",
            "history_policy": policy,
            "embedding": vector,
            "dimension": dimension,
            "user_model_uuid": user_model,
            "post_model_uuid": post_model,
        }
    except RequestError as error:
        # Return per-user failures for aggregate reporting. Configuration and shared
        # contract errors deliberately propagate so the coordinator can stop the run.
        return {"status": "failed", "reason": error.reason}


def average_embeddings(client, dids, workers, expected_source):
    """L2-normalize the equal-weight mean, retaining only aggregate outcome counts."""
    started = time.monotonic()
    next_progress_at = started + 10
    skipped, failed = Counter(), Counter()
    vectors, pending = [], set()
    remaining = iter(dids)
    model_pair = dimension = policy = None
    fatal = None
    logger.info(
        "Embeddings: requesting %d users with workers=%d; timeout=60s attempts=3",
        len(dids),
        workers,
    )
    # Keep only one pending task per worker instead of queuing the whole cohort.
    # That lets a global failure stop new requests while active requests finish.
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for _ in range(workers):
            did = next(remaining, None)
            if did is not None:
                pending.add(executor.submit(fetch_embedding, client, did, expected_source))
        while pending:
            done, pending = wait(pending, timeout=10, return_when=FIRST_COMPLETED)
            for future in done:
                try:
                    result = future.result()
                    status = result["status"]
                    if status == "failed":
                        failed[result["reason"]] += 1
                    else:
                        # The first response establishes the history policy; the
                        # first vector establishes the model pair and dimension.
                        # Later vectors must describe the same embedding space.
                        current_policy = result["history_policy"]
                        if policy is not None and current_policy != policy:
                            raise RunError("Embedding API: mixed history policies")
                        policy = current_policy
                        if status == "skipped":
                            skipped[result["reason"]] += 1
                        else:
                            current_pair = (
                                result["user_model_uuid"],
                                result["post_model_uuid"],
                            )
                            current_dimension = result["dimension"]
                            if model_pair is not None and (
                                current_pair != model_pair or current_dimension != dimension
                            ):
                                raise RunError(
                                    "Embedding API: mixed model pairs or vector dimensions"
                                )
                            model_pair, dimension = current_pair, current_dimension
                            vectors.append(result["embedding"])
                except (RunError, ArtifactValidationError) as error:
                    fatal = fatal or RunError(str(error))
                    failed[str(error)] += 1
                except Exception as error:
                    fatal = fatal or RunError(
                        f"Unexpected embedding failure ({type(error).__name__})"
                    )
                    failed["unexpected_error"] += 1
            now = time.monotonic()
            if now >= next_progress_at:
                logger.info(
                    "Embeddings: progress %d/%d completed contributing=%d skipped=%d "
                    "failed=%d retries=%d elapsed=%.2fs",
                    len(vectors) + skipped.total() + failed.total(),
                    len(dids),
                    len(vectors),
                    skipped.total(),
                    failed.total(),
                    getattr(client, "retry_count", 0),
                    now - started,
                )
                next_progress_at = now + 10
            if fatal is None:
                # Refill completed slots only when no global error was observed.
                for _ in done:
                    did = next(remaining, None)
                    if did is not None:
                        pending.add(executor.submit(fetch_embedding, client, did, expected_source))
    logger.info(
        "Embeddings: summary eligible=%d contributing=%d skipped=%d failed=%d retries=%d",
        len(dids),
        len(vectors),
        skipped.total(),
        failed.total(),
        getattr(client, "retry_count", 0),
    )
    for label, reasons in (("skipped", skipped), ("failed", failed)):
        for reason, count in sorted(reasons.items()):
            logger.warning(
                "Embeddings: %s reason=%s count=%d. %s",
                label,
                reason,
                count,
                REASON_HINTS.get(reason, "Inspect API/inference logs."),
            )
    # Expected history skips are allowed. Any request failure prevents publishing
    # a partial mean, even if other users produced valid vectors.
    if fatal:
        raise fatal
    if failed:
        raise RunError(f"{failed.total()} embedding requests failed; no average was written")
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
    parser.add_argument("--likes-index", default="likes")
    parser.add_argument("--min-likes", type=nonnegative_int, default=5)
    parser.add_argument("--api-url", type=base_url, default="http://localhost:8300")
    parser.add_argument("--workers", type=positive_int, default=4)
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
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", args.likes_index):
        raise RunError("--likes-index must be a single index or alias name")
    for name in ("POSTHOG_PERSONAL_API_KEY", "GE_ELASTICSEARCH_API_KEY", "GE_API_KEY"):
        value = os.environ.get(name, "").strip()
        if not value or any(character.isspace() for character in value):
            raise RunError(f"Set {name} to a valid API key")
        credentials[name] = value
    posthog = JsonClient(
        "PostHog",
        args.posthog_host,
        {"Authorization": f"Bearer {credentials['POSTHOG_PERSONAL_API_KEY']}"},
    )
    # The local-tunnel TLS exception is scoped to ES. PostHog and the embedding
    # API still use their own clients with certificate verification enabled.
    es = JsonClient(
        "Elasticsearch",
        args.es_url,
        {"Authorization": f"ApiKey {credentials['GE_ELASTICSEARCH_API_KEY']}"},
        insecure=args.es_insecure,
    )
    api = JsonClient("Embedding API", args.api_url, {"X-API-Key": credentials["GE_API_KEY"]})
    # Freeze the PostHog cutoff once, rounded down to whole seconds, for every page.
    cutoff = utc_string(started_at.replace(microsecond=0))
    try:
        identity = es.get("/")
        cluster = identity.get("cluster_uuid")
        if (
            not isinstance(cluster, str)
            or not re.fullmatch(r"[A-Za-z0-9_-]+", cluster)
            or cluster == "_na_"
        ):
            raise RunError("Elasticsearch did not return a valid cluster UUID")
        source = {"es_cluster_uuid": cluster, "likes_index": args.likes_index}
        users = collect_posthog_users(
            posthog, args.posthog_project_id, args.min_interaction_seen, cutoff
        )
        likes = collect_like_counts(es, args.likes_index, users)
    except RequestError as error:
        raise RunError(f"Cohort collection failed: {error.reason}") from None
    eligible = [did for did in sorted(users) if likes[did] >= args.min_likes]
    logger.info(
        "Elasticsearch: %d users meet like threshold >=%d; %d filtered out",
        len(eligible),
        args.min_likes,
        len(users) - len(eligible),
    )
    result = average_embeddings(api, eligible, args.workers, source)
    skipped_users = result.pop("skipped_users")
    # The consumer receives one mean plus provenance/coverage, never individual
    # DIDs, activity records, credentials, or individual user vectors.
    artifact = {
        "artifact_type": "average_user_embedding",
        "format_version": 1,
        "run_id": run_id,
        "source_completed_at": utc_string(datetime.now(UTC)),
        **result,
        "cohort": {
            "posthog_project_id": args.posthog_project_id,
            "event": "interactionSeen",
            "scope": "all_history_all_feeds",
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
    except (RunError, ArtifactValidationError, RequestError) as error:
        reason = error.reason if isinstance(error, RequestError) else str(error)
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
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03dZ %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"
    )
    formatter.converter = time.gmtime
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    previous_level = logger.level
    logger.setLevel(logging.INFO)
    try:
        summary = run(args)
    finally:
        logger.removeHandler(handler)
        handler.close()
        logger.setLevel(previous_level)
    print(json.dumps(summary, allow_nan=False))
    return 0 if summary["status"] == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
