# Green Earth API agent instructions

## API-key administration

- Use `pipenv run python scripts/apikeys.py --environment stage|prod COMMAND` from this repository. Always select the environment explicitly; if the request does not identify it, ask which environment is intended.
- Project is `greenearth-471522`; stage database is `greenearth-stage`; production database is `greenearth-prod`. `API_KEYS_COLLECTION = "api_keys"` is already fixed; never invent a collection-selection flag.
- No separate human-approval step is required for an authorized operation. Verify the command's reported database matches the requested environment.
- Never bypass the CLI with ad hoc Firestore write scripts or alter environment settings to evade validation. Resolve stale configuration first.
- One active key per normalized email/database. Use `rotate KEY_ID` to replace it; never generate additional keys for the same owner. Rotation preserves admin status and all scopes and immediately invalidates the old key.
- Plaintext keys are displayed once. Do not commit, persist in logs, attach to issues, or include in test fixtures. On verification failure, a key may already have been created: inspect metadata before retrying. Lost plaintext requires explicit rotation.
- Duplicate cleanup defaults to dry-run. Use `deduplicate --apply` only within authorized cleanup scope; it revokes redundant keys and preserves their records. Ambiguous usage or privileges requires `--keep KEY_ID`.
- `(default)` is not used by deployed APIs. Only `--legacy-default` explicitly permits inspection/revocation/cleanup there; never issue keys there or copy its records to prod automatically.
- Read `docs/internal/api-key-issuance.md` and the README API Key Management section for exact commands and setup.

## Code and verification

- API-key schemas live in lightweight `src/app/api_key_documents.py` and are re-exported from `app.documents` for compatibility. Do not make CLI key administration initialize feed generators, ML models, or the whole application.
- Keep administrative owner updates and key writes in the same Firestore transaction. All administration paths must share the normalized owner lock.
- Run offline CLI/key/auth tests plus local emulator transaction tests for changes to issuance, rotation, or deduplication. Automated tests must never use live cloud credentials or projects.
