# Seriema

Seriema is an event-driven incident handling service for ingesting alerts, matching rules, creating incidents, dispatching notifications, and tracking operational health.

## Visao Geral

The system receives incoming events, evaluates active rules, persists incidents and audit logs in PostgreSQL, and uses Redis + Celery for asynchronous dispatch, retries, replay, and operational snapshots.

### Core Goals

- Detect and persist incidents from incoming events.
- Route notifications by channel.
- Support ACK, resolve, escalation, and callback workflows.
- Expose operational metrics and readiness checks for go-live.

## Stack

- FastAPI
- SQLAlchemy
- Alembic
- PostgreSQL
- Redis
- Celery
- Pydantic
- Pytest

## Arquitetura Resumida

- API: validates requests, writes incidents/audit logs, and exposes operational endpoints.
- Worker: handles dispatch, retries, replay, queue snapshots, DLQ maintenance, and sweepers.
- PostgreSQL: source of truth for incidents, rules, notifications, contacts, groups, and audits.
- Redis: queue broker, short-lived operational state, dedupe, DLQ, replay report, and heartbeats.
- Metabase: reads reporting views and operational summaries from PostgreSQL.

## Setup Local

### Variaveis principais

```bash
APP_BASE_URL=http://localhost:8000
REDIS_URL=redis://localhost:6379/0
DATABASE_URL=postgresql+psycopg://user:pass@localhost:5432/seriema
SERIEMA_DB_SCHEMA=seriema
SERIEMA_REDIS_DB=5
SERIEMA_REDIS_KEY_PREFIX=seriema
SERIEMA_QUEUE_PREFIX=queue:seriema
SERIEMA_OPS_MAX_LIMIT=100
SERIEMA_ADMIN_TOKEN=change-me
VOICE_WEBHOOK_SECRET=change-me
```

Optional operational knobs:

```bash
SERIEMA_DLQ_REPLAY_DRY_RUN=false
SERIEMA_DLQ_REPLAY_BATCH_SIZE=20
SERIEMA_DLQ_MAX_ITEMS=1000
SERIEMA_METRICS_SNAPSHOT_INTERVAL_SECONDS=60
SERIEMA_DLQ_REPLAY_INTERVAL_SECONDS=300
SERIEMA_DLQ_PRUNE_INTERVAL_SECONDS=300
SERIEMA_INCIDENT_STALE_SWEEP_INTERVAL_SECONDS=300
```

## Executando a API

```bash
py -3 -m uvicorn Seriema.main:app --reload
```

## Executando o Worker

```bash
celery -A Seriema.worker.celery_app worker -l info -B
```

## Migracoes Alembic

```bash
py -3 -m alembic -c alembic.ini upgrade head
py -3 -m alembic -c alembic.ini revision --autogenerate -m "descriptive message"
```

## Testes

```bash
py -3 -m pytest tests -q
```

## Validacao de Release

```bash
powershell -ExecutionPolicy Bypass -File scripts/pre_release_check.ps1
powershell -ExecutionPolicy Bypass -File scripts/smoke_e2e_handoff.ps1
```

## Endpoints Operacionais

### Health

- `GET /health`
- `GET /health/deps`

### Metrics

- `GET /metrics/sla`
- `GET /metrics/queues`
- `GET /metrics/ops`
- `GET /alerts/ops`

### Integration and Readiness

- `GET /ops/integration/status`
- `GET /ops/readiness`

### DLQ

- `GET /ops/dlq/preview`
- `POST /ops/dlq/replay`
- `GET /ops/dlq/replay/last`

### Incidents

- `GET /incidents`
- `GET /incidents/{incident_id}`
- `GET /incidents/{incident_id}/timeline`
- `POST /incidents/{incident_id}/ack`
- `POST /incidents/{incident_id}/resolve`

### Rules

- `GET /rules`
- `POST /rules`
- `PATCH /rules/{rule_id}`
- `POST /rules/{rule_id}/toggle`
- `POST /rules/{rule_id}/simulate`

### Contacts

- `GET /contacts`
- `POST /contacts`
- `GET /contacts/{contact_id}`
- `PATCH /contacts/{contact_id}`
- `DELETE /contacts/{contact_id}`

### Groups and Members

- `GET /groups`
- `POST /groups`
- `GET /groups/{group_id}`
- `PATCH /groups/{group_id}`
- `DELETE /groups/{group_id}`
- `POST /groups/{group_id}/members`
- `GET /groups/{group_id}/members`
- `DELETE /groups/{group_id}/members/{contact_id}`

## Docs Relacionados

- [Runbook Operacional](plans/runbook_ops.md)
- [Release Checklist](plans/release_checklist.md)
- [Production Handoff](plans/production_handoff.md)
- [Architecture Overview](plans/architecture_overview.md)
