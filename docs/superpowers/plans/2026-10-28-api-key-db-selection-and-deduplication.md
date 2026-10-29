# API Key Database Selection and Deduplication Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement this approved issue inline, task by task.

**Goal:** Implement issue #584 and clean up live duplicate active keys as authorized by the user.
**Architecture:** Require an explicit environment in the administration CLI, preserve the existing api_keys collection and authentication format, and use transactional owner records for uniqueness and rotation. Deduplication selects the latest successful use and revokes redundant keys without deleting audit records.
**Tech Stack:** Python 3.13, Firestore async transactions, argparse, pytest, local Firestore emulator.
**Spec:** https://github.com/greenearth-social/api/issues/584 and the user's explicit authorization to implement and revoke duplicates using most recent usage.

## Global Constraints
- Branch: inseon/api-key-db-selection-and-deduplication; work in the clean existing checkout as requested.
- Project greenearth-471522; stage -> greenearth-stage; prod -> greenearth-prod; collection api_keys unchanged.
- No human approval gate, extra-key option, or plaintext persistence in audit reports.
- Revoke redundant keys; retain records and preserve privileges.

## Review Focus
- An unused newly generated key must not outrank a genuinely used key.
- Simultaneous generate/rotate/cleanup must serialize through the same owner document.
- Existing legacy owners without an index must not bypass uniqueness.
- Inherited emulator/config settings must never redirect cloud writes.
- A committed write followed by verification failure must not cause automatic issuance retries.

### Task 1: Explicit CLI destination and lightweight imports
Files: scripts/apikeys.py, scripts/apikeys_test.py, src/app/documents.py, src/app/api_key_documents.py, src/app/lib/api_keys.py.
- [x] Add failing tests for missing environment, conflicts, help/import dependency isolation, normalization, destination output, and verification failure.
- [x] Extract lightweight key schema with compatibility re-export; parse before imports; create client from validated environment without the application-wide Firestore imports.
- [x] Verify CLI tests and existing key/auth tests.

### Task 2: Transactional owner uniqueness, rotation, and cleanup
Files: src/app/lib/api_keys.py, src/app/lib/api_keys_test.py, scripts/apikeys_test.py, scripts/apikeys_emulator_test.py.
Interfaces: create_api_key(db,email,is_admin=False), rotate_api_key(db,key_id), revoke_api_key(db,key_id), reconcile_api_keys(db,apply=False,keep_ids=...).
- [x] Add failing tests for duplicate refusal, normalized owners, latest-used retention, unused/tied timestamps, privilege conflicts, and explicit selection.
- [x] Add deterministic api_key_owners records; transactional legacy-key discovery and atomic create/rotate/revoke/reconcile with a shared owner lock.
- [x] Add CLI rotate and deduplicate dry-run/apply, verification, and separate explicit default-database cleanup mode.
- [x] Run emulator concurrency tests including simultaneous creation, rotation, legacy migration, and stale cleanup state.

### Task 3: Documentation, live cleanup, and final verification
Files: README.md, AGENTS.md, docs/internal/api-key-issuance.md (tracked in API repo; update outer runbook pointer separately if needed).
- [x] Update exact commands, dependency setup, recovery, owner policy, cleanup and agent guidance.
- [x] Run a read-only report for stage/prod/default; apply authorized revocations, using latest successful use; stop automatic selection for never-used/tied keys. The user separately authorized revoking all four unused default-database keys.
- [x] Verify no duplicate active normalized owners remain; preserve Vardon's key and all retained key hashes.
- [x] Run focused tests, emulator tests, lint, and full pytest; review complete diff and record any unrelated failures.

## Execution ledger
- Branch created from clean checkout at 840b994. No worktree added because user requested a branch in this checkout.
- Baseline key tests fail during application import: missing declared dependency jsonschema. Repair the local environment using existing locked dependencies before rerunning.

- Task 1 complete: CLI requires environment, validates inherited configuration, reports the fixed collection/database, and imports lightweight key schemas without feed/ML initialization.
- Task 2 complete: transactional normalized-owner uniqueness, explicit privilege-preserving rotation, owner-aware revoke, and deduplicate dry-run/apply. Real local emulator tests cover concurrent generation/rotation, legacy detection, and usage changes before apply.
- Ruling: scan the small administrative key collection in transactions to discover mixed-case legacy owners reliably; this trades administrative read cost/contention for correctness and is documented (not intended for high-volume public issuance).
- Dependency repair used existing locked jsonschema 4.26.0 and anthropic 1.8.0 locally; no dependency manifests changed.
- Test isolation incident: the initial missing-environment regression subprocess inherited live credentials and created one test record in the default database. Removed that exact record immediately, then blocked live credentials in subprocess tests. No developer keys were affected by this incident.
- Fresh read-only review found missing-timestamp defaults could distort cleanup ranking. Added failing tests for absent created_at/last_used_at and fixed automatic selection to require explicit --keep in those cases.
- Fresh review also found missing python-dotenv escaped dependency error handling. Added a failing import-isolation test and moved that import into the recovery handler; test passes.
- Operational cleanup used the official CLI: revoked 10 duplicate prod keys and 5 duplicate stage keys, retaining the latest successfully used key for each owner. User separately requested removal of all four unused default keys; revoked all four. No physical deletion, key-hash changes, privilege changes, or cross-database copies occurred.
- Post-cleanup verification: prod has 8 active keys, stage has 8, default has 0; all active owners unique and owner indexes consistent. Vardon's existing production key still returns HTTP 200.
- Final validation: full suite with local key emulator enabled: 2328 passed, 4 skipped (unrelated opt-in emulator tests), 159 existing warnings. Full pyright: 0 errors/warnings. Ruff on changed Python files and git diff --check pass.
- Code is kept on the requested local branch; no deployment, merge, or push is part of this task.

- PR preparation: merged current main and resolved overlapping API-key scope changes. Preserved scope defaults/validation, repeatable --scope, scoped CLI output, and scope-specific regressions; added emulator coverage for scope-preserving rotation and explicit retention on conflicting scopes. No further cloud mutations occurred.
- Validation after main integration: 2561 passed, 4 skipped; full pyright reports 0 errors/warnings; Ruff on affected files and diff-versus-main whitespace checks pass.
