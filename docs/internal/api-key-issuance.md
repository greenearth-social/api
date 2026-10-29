# API key issuance runbook

Internal operational guidance. Do not put issued keys or developer emails in public issues.

## Setup and destination

From `api/`, use Python 3.13, `pipenv sync --dev`, and `gcloud auth application-default login`.
The Google application-default credential is separate from the gcloud CLI login.

Always specify `--environment stage` or `--environment prod` before the subcommand.
This selects `greenearth-stage` or `greenearth-prod` in project `greenearth-471522`.
The collection `api_keys` is already hardcoded. Unset conflicting project/database
variables and both `GE_FIRESTORE_EMULATOR_HOST` / `FIRESTORE_EMULATOR_HOST` for cloud work.
No interactive human approval is required by the CLI.

## Issue, inspect, rotate, revoke

```bash
pipenv run python scripts/apikeys.py --environment prod generate developer@example.com
pipenv run python scripts/apikeys.py --environment prod list
pipenv run python scripts/apikeys.py --environment prod usage KEY_ID
pipenv run python scripts/apikeys.py --environment prod rotate KEY_ID
pipenv run python scripts/apikeys.py --environment prod revoke KEY_ID
```

Use `stage` for stage keys. Normal developer keys are non-admin; `generate --admin`
is for the Green Earth team only. Use repeatable `generate --scope NAME` to grant named capabilities. Every key
always includes `default`; admin status does not imply additional scopes. Rotation
preserves admin status and all effective scopes.

Check the printed project/database/API URL. Save and deliver the one-time plaintext
through an appropriate private channel. Use `X-API-Key` to authenticate requests.
Only a hash is retained; lost plaintext requires explicit rotation.

Generate refuses an owner with an active key. Do not bypass it: use rotation, which
immediately deactivates the identified old key. If creation succeeds but API
verification fails, keep the key ID, inspect the destination and service, and do
not generate again. An uncertain write result also requires metadata inspection;
repeating rotation with the old inactive ID must not create another replacement.

## Deduplicate existing records

```bash
pipenv run python scripts/apikeys.py --environment prod deduplicate
pipenv run python scripts/apikeys.py --environment prod deduplicate --apply
pipenv run python scripts/apikeys.py --environment prod deduplicate --keep KEY_ID --apply
```

Dry-run does not write. Apply re-reads usage and key/owner state transactionally;
it keeps the latest successfully used key, revokes others, and seeds owner records
for all active owners. A changed last-used time can change the selected key between
report and apply. Review the apply output.

Creation initializes `last_used_at`; equality to `created_at` with a zero counter
is not evidence of use. Tied usage, never-used duplicates, or different admin
privileges (admin status or scopes) need explicit `--keep KEY_ID`; repeat for different owners. Do not use
creation time alone to replace a genuinely used key. Records remain for audit.

The legacy `(default)` database is separate and not used by deployed APIs:

```bash
pipenv run python scripts/apikeys.py --legacy-default list
pipenv run python scripts/apikeys.py --legacy-default deduplicate --keep KEY_ID --apply
```

It supports inspection/revocation/cleanup only, never generation or rotation.
No automatic migration to production occurs.

## Verification

```bash
pipenv run pytest scripts/apikeys_test.py src/app/lib/api_keys_test.py src/app/security_test.py
# Start a local Firestore emulator separately; tests use unique demo projects and anonymous credentials.
FIRESTORE_API_KEYS_TEST_EMULATOR_HOST=127.0.0.1:8080 pipenv run pytest scripts/apikeys_emulator_test.py
```

Help and argument validation must work without cloud credentials. Never use ad hoc
Firestore writes as a workaround for dependency, destination, or duplicate errors.
