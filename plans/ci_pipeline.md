# CI Pipeline

This pipeline runs on `push` and `pull_request` against `main`.

## What It Does

1. Starts PostgreSQL and Redis service containers.
2. Installs project dependencies plus test tooling.
3. Runs `python -m compileall .` to catch syntax issues early.
4. Runs `python -m pytest tests -q` against the full test suite.
5. Runs `python -m alembic -c alembic.ini upgrade head --sql` in offline mode to verify migration SQL generation without needing a live database connection.

## Local Pre-Release Check

For a one-command validation before release, run:

`powershell -ExecutionPolicy Bypass -File .\\scripts\\pre_release_check.ps1`

The script runs the same three checks in order:

1. `compileall`
2. `pytest -q`
3. `alembic upgrade head --sql`

## Environment

- `DATABASE_URL` points to the local PostgreSQL service.
- `REDIS_URL` points to the local Redis service.
- `SERIEMA_*` variables configure schema, queues, limits, metrics, and webhook hardening.

## Notes

- The workflow installs `pytest` and `httpx` explicitly because they are not listed in `requirements.txt`.
- The offline Alembic step uses example environment values and does not require a live database connection.
- The pre-release script mirrors the CI validation sequence and prints a PASS/FAIL summary for each step.
