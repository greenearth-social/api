# Green Earth API — Agent Notes

FastAPI service (Python 3.13) for Bluesky content recommendations. Managed with `pipenv`.

## Everyday commands

```bash
# Install (editable package + dev deps)
pipenv install --dev

# Dev server
pipenv run uvicorn src.app.main:app --reload      # http://localhost:8000

# Verify (matches CI)
pipenv run ruff check .
pipenv run ruff format --check .
pipenv run pyright
pipenv run pytest -v
```

CI runs `pyright` then `pytest -v`; lint is not enforced in CI but `ruff` is configured in `pyproject.toml`.

## Project layout

- Entry point: `src.app.main:app` (FastAPI). Production uses `Procfile`: `uvicorn src.app.main:app --host 0.0.0.0 --port $PORT`.
- Source lives under `src/app/`, not at repo root.
- Routers: `src/app/routers/` (health, candidates, rank, diversify, skylight, xrpc, feed_debug).
- Shared libs: `src/app/lib/` (Firestore, ES client, inference, metrics, caching, firebase_auth, post_hydration, feed_debug).
- Documents (Firestore Pydantic models): `src/app/documents.py`.
- API response models: `src/app/models.py` (pipeline types), `src/app/models_feed_debug.py` (feed-debug API views).
- Scripts: `scripts/` (deploy, feed publishing, API key management, profiling). Deployed containers exclude `scripts/` via `.gcloudignore`.
- Tests are co-located as `*_test.py` next to the files they test. `pyproject.toml` sets `pythonpath = ["src", "scripts"]`.

## Required environment to start

The app refuses to start without:

- `GE_ELASTICSEARCH_API_KEY`
- `GE_FEED_CONTEXT_SECRET`

Other important variables:

- `GE_ELASTICSEARCH_URL` — defaults to `https://localhost:9200`.
- `GE_ELASTICSEARCH_VERIFY_SSL` — defaults to `false`.
- `GE_FIRESTORE_EMULATOR_HOST` — set to `127.0.0.1:8080` to use the local emulator.
- `GE_FIRESTORE_PROJECT` — use `demo-no-project` with the default emulator.
- `GE_INFERENCE_BASE_URL` — local override; deploys fall back to mapped domains.

See `.env.example` for the full set.

## Local Firestore

```bash
firebase emulators:start --only firestore   # requires firebase-tools / Node
```

Then run the API with:

```bash
GE_FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 \
GE_FIRESTORE_PROJECT=demo-no-project \
GE_ELASTICSEARCH_API_KEY=dummy \
GE_FEED_CONTEXT_SECRET=dummy \
pipenv run uvicorn src.app.main:app --reload
```

The emulator does not persist data across restarts by default; add `--export-on-exit`/`--import` if needed.

## Tests

Most tests are unit tests with mocks and do not require Elasticsearch, Firestore, or the emulator. The shared `src/app/conftest.py` bypasses **both** API-key auth and Firebase auth by default via `dependency_overrides`; `security_test.py` overrides the API-key fixture to test auth behavior.

```bash
# Run everything
pipenv run pytest -v

# Run a single module
pipenv run pytest -v src/app/lib/firestore_test.py

# Run a single test
pipenv run pytest -v src/app/lib/firestore_test.py::test_user_doc_id
```

### Test patterns for Firestore batching

When mocking Firestore calls that use batched I/O, override these on the mock `db` object:

- **Batched reads** (`get_all`): set `db.get_all = AsyncMock(return_value=[mock_doc1, mock_doc2])`. Each mock doc must have `.id` set to the rkey so the `rkey_to_doc` lookup works.
- **Batched writes** (`batch`): mock `db.batch().commit()` — verify `batch.set(ref, data)` was called for each expected write.

Test fixtures that set `dependency_overrides` should **pop only their own override** on teardown (`app.dependency_overrides.pop(fn, None)`), not `clear()` which wipes other fixtures' overrides.

## Firebase Auth (feed-debug API)

The `/api/feeds` endpoints use Firebase custom tokens for auth — separate from the `X-API-Key` system used by pipeline endpoints.

- `src/app/lib/firebase_auth.py` — `init_firebase_auth()` (called in lifespan, wrapped in try/except so missing ADC doesn't block startup), `verify_firebase_auth()` (FastAPI dependency via `HTTPBearer`), `FirebaseUser` type alias.
- Token flow: frontend sends `Authorization: Bearer <firebaseCustomToken>`, `firebase_admin.auth.verify_id_token` decodes the `uid` (DID), `user_doc_id()` strips the `did:plc:` prefix → Firestore document key.
- `init_firebase_auth()` failure is non-fatal — the feed-debug endpoints return 500 but the rest of the API works.

## Post hydration

`src/app/lib/post_hydration.py` fetches post metadata (author, media, engagement) from the Bluesky public API with a Firestore-backed cache.

- **Batch reads**: `get_cached_hydrated_posts` uses `db.get_all([refs])` — one round-trip for all URIs.
- **Batch writes**: `cache_hydrated_posts` uses `db.batch()` — single-commit write batch.
- `_parse_bsky_post` extracts author, content, media (images/video/link card), and engagement counts.
- `hydrate_posts` is the main entry point: cache-first, Bluesky API for misses (max 25 per batch), empty fallback for unresolvable URIs.

## Feed snapshots (pipeline metadata)

Every feed load writes a lightweight `FeedSnapshotDocument` to `users/{user_did}/feed_snapshots/{request_id}` via `write_feed_snapshot()` — stored inline (not background) so the transparency API works for all users, not just debug-flagged ones. The full debug document (`FeedDebugDocument`) is still gated on `debug_feeds` and written in background.

- `FeedSnapshotDocument` / `PipelineItemMeta` / `DiversificationMeta` / `GeneratorMeta` / `ModelScoreMeta` are defined in `src/app/documents.py`.
- `FeedDebugRecorder.build_pipeline_metadata()` in `src/app/lib/feed_debug.py` assembles the snapshot from in-memory recorder state.
- `firestore.indexes.json` has a composite index on `feed_snapshots (feed_name ASC, generated_at DESC)` for the `get_recent_feed_snapshots` query with `feed_name` + `cutoff` filters.

## camelCase serialization

API response models in `src/app/models_feed_debug.py` use a `CamelModel` base class with `alias_generator=_to_camel` and `populate_by_name=True`. All field names are snake_case in Python, serialized as camelCase in JSON. No separate mapping layer needed.

## API keys

Keys are stored in Firestore and issued via `scripts/apikeys.py`:

```bash
# Against the emulator
GE_FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 \
  pipenv run python scripts/apikeys.py generate alice@example.com

# Against a real project
GE_FIRESTORE_PROJECT=greenearth-471522 GE_FIRESTORE_DATABASE=greenearth-stage \
  pipenv run python scripts/apikeys.py generate alice@example.com
```

**Deploy order matters:** generate at least one key in the target Firestore database *before* deploying, or every request returns 401.

## Deployment

```bash
./scripts/deploy.sh                  # stage
ENVIRONMENT=prod ./scripts/deploy.sh # prod
```

What the deploy script does:

- Auto-detects the Elasticsearch internal load balancer IP via `kubectl` (or use `--elasticsearch-url`).
- Deploys `firestore.rules` and `firestore.indexes.json` with `firebase deploy --only firestore`.
- Generates `requirements.txt` from `Pipfile` via `pipenv requirements` for Cloud Buildpacks.
- Deploys to Cloud Run as `greenearth-api-<environment>`.
- Syncs feed generator records via `scripts/publish_feed.py`.

Inference endpoint resolution:

1. `GE_INFERENCE_BASE_URL` / `--inference-base-url`
2. Mapped domain: `inference-stage.greenearth.social` (stage) or `inference.greenearth.social` (prod)
3. If mapping is disabled and no base URL is set, `two_tower` calls fail.

Prod feed generator DID is fixed at `did:web:api.greenearth.social`; stage derives it from the Cloud Run service URL.

## Feed generator development

Bluesky needs to reach your local server over the public internet. Expose it (Tailscale Funnel, ngrok, etc.), then publish:

```bash
pipenv run python scripts/publish_feed.py \
  --handle caterpie-internal.bsky.social \
  --feed-name unranked-your-feed \
  --environment dev \
  --app-password "$GE_BSKY_APP_PASSWORD"
```

Set `GE_FEED_GENERATOR_DID` to the public hostname (e.g. `did:web:your-machine.tail1234.ts.net`).

## Auth / routing notes

- Pipeline endpoints (`/candidates`, `/rank`, `/diversify`, `/skylight`, `/`) require `X-API-Key`.
- `/health`, AT Protocol `/xrpc/...`, and `/.well-known/did.json` are public.
- `/api/feeds` endpoints require `Authorization: Bearer <firebaseToken>`.
- Middleware order matters: profiling middleware is registered first so request-ID middleware runs outside it.

## Branches

CI runs on pushes and PRs to `main` and `develop`.
