# Simulated user load testing (api#189)

Drive realistic feed-request load at the Green Earth API using a set of **real
Bluesky users**, so we can measure how the retrieve→rank pipeline behaves under
concurrency — including the cold-start path — while keeping every byte of test
traffic and test-created data cleanly separable from real users.

Four scripts, run in order:

| Step | Script | What it does |
|---|---|---|
| 1 | `select_users.py` | Pick a diverse cohort of real user DIDs → JSON manifest |
| 2 | `run.py` | Generate load (getFeedSkeleton + paging + sendInteractions), write per-request JSONL |
| 3 | `analyze.py` | Report client-side latency from the JSONL, plus a deep-link to the bottleneck-attribution dashboard |
| 4 | `cleanup.py` | Delete the data the run created, restoring the pre-run baseline |

## How test traffic is isolated

Requests carry two headers — `X-Load-Test-Secret` (a shared secret) and
`X-Load-Test-DID` (the user to act as) — which make the API skip AT Protocol
auth for that request. The session is then treated as a **real** session so the
full write path is exercised, but everything it produces is tagged so it can be
found and removed afterward, and analytics (PostHog) are skipped.

The tag mechanism differs by data shape (see the doctrine in
`src/app/routers/xrpc.py` and the PR #315 discussion):

- **User docs** — `created_by_load_test: true` field. A user who later makes a
  real request has the flag cleared ("becomes ours") and is never deleted.
- **Interactions / feed_cache** — `load_test: true` field.
- **seen_posts / discarded_posts daily buckets** — a **suffixed doc ID**
  (`YYYY-MM-DD-load-test`) in the *same* subcollection as real buckets, so the
  exclusion read path is exercised identically while the test data stays
  structurally separate and trivially deletable. This holds even when the user
  is a real one who happened to be in the cohort.
- **feed_snapshots** — written then **immediately deleted** for load-test
  traffic (the transparency view is their only reader), so they exercise the
  transactional write but leave nothing behind.

The bypass is **off unless the server has `GE_LOAD_TEST_SECRET` set**. Absent
that env var, the headers do nothing and the request is treated as anonymous.

## Prerequisites

- **Server side:** the target API (Cloud Run for stage/prod) must have
  `GE_LOAD_TEST_SECRET` set. The client scripts read the matching secret from
  `--secret`, then `$GE_LOAD_TEST_SECRET`, then Secret Manager
  (`load-test-secret-<env>`).
- **`gcloud` auth:** `select_users.py` and `cleanup.py` talk to Firestore — run
  `gcloud auth application-default login` first.
- **Elasticsearch:** `select_users.py` queries ES for the active/low cohorts.
  For stage/prod, port-forward it:
  ```bash
  kubectl port-forward service/greenearth-es-http 9200:9200 -n greenearth-prod
  ```
  Pass `--es-password "$ELASTIC_PASSWORD"` (the `elastic` user's password from
  the `greenearth-es-elastic-user` secret) or `--es-api-key`.

Run everything from the `api/` directory (`pipenv run …`).

## Workflow (stage example)

### 1. Select users

```bash
pipenv run python scripts/load_test/select_users.py \
    --environment stage \
    --count 100 \
    --es-password "$ELASTIC_PASSWORD" \
    --output load_test_users.json
```

The cohort mix (default 60/30/10) spans warm and cold serving paths:

- `existing` — our own feed users, from the Firestore `users` collection (warm).
- `active` — active Bluesky users with recent like history in ES but no
  Firestore doc yet (warm ES, cold Firestore — exercises user creation).
- `low` — users with little/no like history (fully cold).

Tune with `--pct-existing/--pct-active/--pct-low` (must sum to 100),
`--days` (like-history window), `--low-likes-max`, and `--seed`. If a cohort
pool is too small the script under-fills and warns.

### 2. Generate load

```bash
pipenv run python scripts/load_test/run.py \
    --users load_test_users.json \
    --environment stage \
    --feed your-feed:60,random:25,best-of-friends:15 \
    --rate 60 --duration 10 \
    --out results.jsonl
```

- `--rate` sessions/minute, `--duration` minutes. Rates above 300/min require
  `--force` (guardrail against fat-fingering prod).
- Each session does an initial fetch, samples a page depth (`--mean-pages`,
  geometric), pauses `--think-time-ms` between pages, and — with probability
  `--interaction-share` — reports seen/like/click interactions for the items it
  saw, echoing each item's `feedContext` token verbatim.
- `--feed` picks which feed(s) to exercise. A single rkey (`your-feed`) sends
  everyone to one feed; a weighted list buckets users across several —
  `--feed your-feed:60,random:25,best-of-friends:15`. Each **user** is pinned to
  one feed for the whole run (real users mostly stick to a single feed), and the
  weights set the fraction of users assigned to each. Weights are relative (need
  not sum to 100); bare rkeys get equal weight. Only publicly-served feeds work
  — the rkeys must appear in `describeFeedGenerator`. The plan output shows the
  resulting per-feed user counts.
- `--api-url` overrides URL resolution; `--secret` overrides secret resolution.
- `--dry-run` prints the schedule and feed assignment, and sends nothing.

Prints a client-side latency summary and writes one JSONL record per HTTP call
(each stamped with the feed it hit).

> **WARNING:** against prod this competes with real user traffic. Run only in a
> low-traffic window, and start small.

### 3. Analyze

```bash
pipenv run python scripts/load_test/analyze.py \
    --results results.jsonl --environment stage
```

Reports client-side latency by feed × phase × cohort, then prints a
console deep-link to the bottleneck-attribution Cloud Monitoring dashboard
with the run window pre-set (`feed.render.duration_ms` percentiles by
`traffic` class, Cloud Run log/error counts, and the rest of the
attribution playbook — see
[`monitoring/README.md`](../../monitoring/README.md)). The link requires a
dashboard to have been deployed for the target environment
(`monitoring/deploy.sh <env>`, which writes `monitoring/dashboards/ids.env`);
without that file — or without an entry for the target environment —
`analyze.py` prints a hint to run it instead of a link.

`analyze.py` **reads nothing from Firestore** — all the context it needs is
stamped on each JSONL record — so you can run cleanup *before* analysis if you
like. Accepts multiple `--results` files.

### 4. Cleanup

The goal is always to remove **everything** the run created. Dry run by default;
`--execute` actually deletes.

```bash
# Preview
pipenv run python scripts/load_test/cleanup.py --environment stage --users load_test_users.json
# Delete
pipenv run python scripts/load_test/cleanup.py --environment stage --users load_test_users.json --execute
```

What it removes, all in one pass:

- **Load-test-created users** (`created_by_load_test == true`), whole subtree
  via `recursive_delete`. Re-checks the flag immediately before deleting, so a
  user who became real in the meantime is skipped.
- **Interactions** tagged `load_test`.
- **feed_cache** tagged `load_test` (unless `--skip-cache`).
- **Suffixed activity buckets** (`…-load-test`) belonging to *real* users named
  in the manifest — the case `recursive_delete` doesn't cover.

**`--users` is a hint, not a scope.** Everything tagged is cleaned globally
regardless. The manifest exists only because one thing can't be found by tag: a
load-test activity bucket that accumulated under a *real* user (its suffixed doc
ID sits in the same subcollection as that user's real buckets). The manifest
tells cleanup which real users to check for those.

**Without `--users`, cleanup can't be complete**, so it refuses unless you pass
`--force` — which cleans everything else and leaves those real-user buckets to
native TTL (14d/3d). In practice you always have the manifest from
`select_users.py`, so pass it and let cleanup be exhaustive.

## Running against the local devenv

The bypass is off in dev by default. To smoke-test the whole flow locally:

1. **Enable the secret on the api.** Add to the `api` service's `environment:`
   in `internal-tools/devenv/docker-compose.yml`:
   ```yaml
   - GE_LOAD_TEST_SECRET=${GE_LOAD_TEST_SECRET:-ge-dev-load-test}
   ```
   then `devctl restart api`. (Remove it again when done.)

2. **Select** — dev reads the local Firestore emulator and dev ES. Dev has few
   Firestore users, so bias toward the ES cohorts:
   ```bash
   devctl exec api pipenv run python scripts/load_test/select_users.py \
       --environment dev --count 15 --pct-existing 20 --pct-active 60 --pct-low 20 \
       --es-url http://elasticsearch:9200 --output scripts/load_test/users.json
   ```
   (The ES API key comes from the container's `GE_ELASTICSEARCH_API_KEY`.)

3. **Run** — point at the api's in-container port and pass the dev secret:
   ```bash
   devctl exec api pipenv run python scripts/load_test/run.py \
       --users scripts/load_test/users.json \
       --api-url http://127.0.0.1:8000 --secret ge-dev-load-test \
       --rate 20 --duration 1 --interaction-share 0.8 \
       --out scripts/load_test/results.jsonl
   ```

4. **Analyze**, then **cleanup** — one pass — with `--environment dev`
   (cleanup fails closed unless a Firestore emulator host is set; `devctl exec`
   provides one). Dev has no dashboard deployed, so `analyze.py` prints the
   client-side report and a hint instead of a dashboard link:
   ```bash
   devctl exec api pipenv run python scripts/load_test/analyze.py \
       --results scripts/load_test/results.jsonl
   devctl exec api pipenv run python scripts/load_test/cleanup.py \
       --environment dev --users scripts/load_test/users.json --execute
   ```

Inspect emulator state before/after in the Firestore emulator UI
(http://localhost:4000/firestore) to confirm cleanup restored the baseline.
```
