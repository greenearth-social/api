# Green Earth API

Python 3.13 FastAPI service for Bluesky feed recommendations. Use Pipenv.

## Where to work

- `src/app/routers/`: HTTP endpoints and request handling.
- `src/app/lib/`: reusable service logic and persistence helpers.
- Tests usually live beside the code they exercise, with names ending in `_test.py`.
- `scripts/`: development and operational tools. Check a script's effects before running it.

Before starting the server, read the README's configuration requirements and
`.env.example`. A local server can still connect to live services.

## Verification

After dependency changes or switching to a branch with a different lockfile,
run `pipenv sync --dev` to synchronize the environment.

For behavior changes, add regression coverage and run the relevant tests while
working. Before opening or updating a code PR, run the checks defined in
`.github/workflows/python-ci.yml`:

```bash
pipenv run python -m pip check
pipenv run pyright
pipenv run pytest
```

Run Ruff on changed Python files using the configuration in `pyproject.toml`.
For documentation-only edits, verify referenced paths and commands; a code test
run is unnecessary unless the edit changes executable examples or configuration.

Use mocks or a local emulator for automated tests. Ensure test subprocesses do
not inherit live cloud credentials.

Report which checks ran and their results. Identify any checks that could not
run and explain why; do not claim that unexecuted checks passed.

## Operational boundaries

A request to change code does not itself authorize deployment, credential
administration, or live data changes. Those operations require an explicit
request covering the operation and target environment. Ask only for missing
information; existing authorization does not require repeated confirmation.

Use the documented operational tools. Resolve configuration conflicts rather
than bypassing their validation with ad hoc write scripts.

Keep credentials out of commits, persistent logs, issues, and test fixtures.
An ignored `.env` file can still be read by a process; `.gitignore` does not
restrict access to credentials.

## Specialized guidance

Before changing or using API-key administration, read
`docs/internal/api-key-issuance.md` and the README's API Key Management section.
Keep issuance procedures and environment mappings in those documents rather
than duplicating them here.

## Pull requests

Explain the problem, resulting behavior, validation, and any live operational
changes already performed. Exclude temporary session notes and execution ledgers.
