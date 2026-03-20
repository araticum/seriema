# Production Handoff

This document is the practical handoff for running Seriema in production.

## Deployment Steps

1. Confirm the target PostgreSQL and Redis services are reachable.
2. Set the required environment variables for schema, queues, and secrets.
3. Run `alembic upgrade head` against the target database.
4. Start the API service.
5. Start Celery workers and the beat scheduler.
6. Verify `/health`, `/health/deps`, `/metrics/sla`, `/metrics/queues`, `/ops/integration/status`, and `/ops/readiness`.
7. Run the smoke handoff check: `powershell -ExecutionPolicy Bypass -File .\scripts\smoke_e2e_handoff.ps1`.
8. Send one test event and confirm the full ingest-to-ack flow.
9. Verify Metabase dashboards read from the `seriema` views.

## Required Environment Variables

- `DATABASE_URL`
- `REDIS_URL`
- `SERIEMA_DB_SCHEMA`
- `SERIEMA_REDIS_DB`
- `SERIEMA_REDIS_KEY_PREFIX`
- `SERIEMA_QUEUE_PREFIX`
- `APP_BASE_URL`
- `SERIEMA_API_BASE_URL` for the smoke handoff script when the API is not on `127.0.0.1:8000`

## Required When Enabled

- `VOICE_WEBHOOK_SECRET`
- `SERIEMA_ADMIN_TOKEN`

## Worker and Ops Settings

- `SERIEMA_TASK_MAX_RETRIES`
- `SERIEMA_TASK_RETRY_BACKOFF`
- `SERIEMA_TASK_RETRY_BACKOFF_MAX`
- `SERIEMA_TASK_RETRY_JITTER`
- `SERIEMA_TASK_SOFT_TIME_LIMIT`
- `SERIEMA_TASK_TIME_LIMIT`
- `SERIEMA_DLQ_QUEUE_NAME`
- `SERIEMA_DLQ_REPLAY_BATCH_SIZE`
- `SERIEMA_DLQ_MAX_ITEMS`
- `SERIEMA_DLQ_REPLAY_DRY_RUN`
- `SERIEMA_METRICS_KEY`
- `SERIEMA_METRICS_TTL_SECONDS`
- `SERIEMA_METRICS_SNAPSHOT_INTERVAL_SECONDS`
- `SERIEMA_DLQ_REPLAY_INTERVAL_SECONDS`
- `SERIEMA_DLQ_PRUNE_INTERVAL_SECONDS`
- `SERIEMA_OPS_MAX_LIMIT`
- `VOICE_WEBHOOK_MAX_AGE_SECONDS`

## Recommended Defaults

- Keep PostgreSQL schema isolated under `seriema`.
- Keep Redis DB isolated from other services.
- Keep queue and key prefixes distinct from the rest of the stack.
- Keep `VOICE_WEBHOOK_SECRET` enabled in any environment that accepts real callbacks.
- Keep `SERIEMA_ADMIN_TOKEN` enabled for operational endpoints outside local dev.

## Suggested SLOs

- API availability: `99.9%`
- Dependency health endpoint success: `99.9%`
- Event ingest to incident persistence: `p95 < 1s`
- Voice acknowledgement time to detect: `p95 < 5s`
- Worker queue backlog: investigate if dispatch or voice queue stays above threshold for more than 10 minutes
- DLQ size: investigate immediately if growth is sustained for more than 10 minutes

## Daily Operations

1. Check `/health/deps`.
2. Review `/metrics/queues`.
3. Review `/metrics/ops`.
4. Check `/metrics/sla?hours=24`.
5. Review `/ops/integration/status`.
6. Review `/ops/readiness`.
7. Inspect `/ops/dlq/preview` if the DLQ is non-empty.
8. Confirm Metabase dashboards still show the expected 24h trend.
9. Review logs for `FAILED`, `ESCALATED`, and `ACK_RECEIVED`.

## Troubleshooting

### DLQ Growing

- Inspect `/ops/dlq/preview`.
- Identify the repeated `task_name` and `error`.
- Use `SERIEMA_DLQ_REPLAY_DRY_RUN=true` for a safe rehearsal.
- Fix the underlying issue before replaying.
- Prune only after replay or after deciding the item is permanently bad.

### Queue Backlog

- Check `/metrics/queues`.
- If `dispatch` grows, verify rule matching and DB latency.
- If `voice` grows, verify provider latency and webhook handling.
- If `telegram` or `email` grows, inspect provider credentials and worker health.

### Callback Failures

- Confirm `VOICE_WEBHOOK_SECRET` is configured if signatures are enabled.
- Confirm the timestamp is within `VOICE_WEBHOOK_MAX_AGE_SECONDS`.
- Check `X-Voice-Timestamp` and `X-Voice-Signature`.
- Confirm the callback path matches `APP_BASE_URL`.

### Migration Issues

- If a migration fails, stop workers before retrying the schema change.
- Confirm the current revision with `alembic current`.
- Re-run `alembic upgrade head` only after resolving the cause.
- If a schema rollback is required, roll back one revision at a time.

## Pre-Deploy Checklist

- `alembic upgrade head` succeeds in the target environment.
- `GET /health` returns `200`.
- `GET /health/deps` reports PostgreSQL and Redis as `ok`.
- `GET /metrics/sla` and `GET /metrics/queues` return valid payloads.
- `GET /metrics/ops` returns a populated or empty metrics hash cleanly.
- `GET /alerts/ops` returns a valid alert snapshot.
- `GET /ops/integration/status` returns a consistent handoff signal.
- `GET /ops/readiness` returns a readiness score and blockers list.
- `GET /ops/dlq/preview` works with the configured admin token.
- `GET /ops/dlq/replay/last` returns the latest replay report shape.
- Workers start with the expected queues and beat schedule.

## Post-Deploy Checklist

- Send a test event.
- Confirm incident creation.
- Confirm queueing and notification dispatch.
- Confirm callback acknowledgement works in the test environment.
- Confirm Metabase views are present and queryable.
- Confirm DLQ stays bounded and pruning is active.
- Run `powershell -ExecutionPolicy Bypass -File .\scripts\smoke_e2e_handoff.ps1` and keep the report for handoff.

## Handoff Notes

- Keep the service isolated logically even when sharing infrastructure.
- Do not let other services reuse the Seriema Redis DB or queue prefix.
- Keep the audit log as the operational record of truth.
- Treat DLQ preview and dry-run as the first step before any real replay.
