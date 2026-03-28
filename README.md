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

Copie o template de ambiente:

```bash
cp .env.example .env
```

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
VOICE_PRERECORDED_AUDIO_URL=https://cdn.example.com/incident-alert.mp3
VOICE_PROVIDER=signalwire
VOICE_TWIML_MODE=dynamic
SIGNALWIRE_SPACE_URL=your-space.signalwire.com
SIGNALWIRE_PROJECT_ID=
SIGNALWIRE_API_TOKEN=
SIGNALWIRE_FROM_NUMBER=
TWILIO_ACCOUNT_SID=
TWILIO_AUTH_TOKEN=
TWILIO_FROM_NUMBER=
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
py -3 -m uvicorn seriema.main:app --reload
```

## Executando o Worker

```bash
celery -A seriema.worker.celery_app worker -l info -B \
  -Q queue:seriema:dispatch,queue:seriema:voice,queue:seriema:telegram,queue:seriema:email,queue:seriema:dlq
```

> Importante: a API publica tasks nas filas prefixadas por `SERIEMA_QUEUE_PREFIX` (por padrao `queue:seriema:*`). Se o worker ouvir apenas `dispatch,voice,...`, os eventos ficam aceitos com `RULE_MATCHED`, mas o dispatcher nunca consome a task.

## Migracoes Alembic

```bash
py -3 -m alembic -c alembic.ini upgrade head
py -3 -m alembic -c alembic.ini revision --autogenerate -m "descriptive message"
```

## Testes

Runbook local padronizado: [`docs/testing-local.md`](docs/testing-local.md)

Antes de testar em clone local/descartável (`_local/seriema`), mantenha o ambiente sincronizado com o remoto:

```bash
git fetch origin
git pull --ff-only origin main
rm -rf .venv
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
```

Depois rode:

```bash
./scripts/test-local.sh
```

O `pyright` desse fluxo está configurado com escopo pragmático via `pyrightconfig.json`: ele valida primeiro os módulos-base do runtime que já estão tipáveis (`classifier.py`, `config.py`, `database.py`, `models.py`, `redis_client.py`, `schemas.py`) e deixa `main.py`/`worker.py`/`observability.py`/migrações/testes fora do gate até o backlog real ser tratado em etapas.

Equivalente manual:

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
- `GET /health/integrations`

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

### Voice

- `GET|POST /dispatch/voice/twiml/{notification_id}`
- `GET|POST /dispatch/voice/twiml/prerecorded/{notification_id}`
- `POST /dispatch/voice/callback/{notification_id}`

Para SignalWire (principal):

- defina `VOICE_PROVIDER=signalwire`
- configure `SIGNALWIRE_SPACE_URL`, `SIGNALWIRE_PROJECT_ID`, `SIGNALWIRE_API_TOKEN`, `SIGNALWIRE_FROM_NUMBER`
- opcionalmente use `VOICE_TWIML_MODE=prerecorded` com `VOICE_PRERECORDED_AUDIO_URL`

Para Twilio (alternativa compatível):

- defina `VOICE_PROVIDER=twilio`
- configure `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_FROM_NUMBER`

### Rules

- `GET /rules`
- `POST /rules`
- `PATCH /rules/{rule_id}`
- `POST /rules/{rule_id}/toggle`
- `POST /rules/{rule_id}/simulate`

## Integracao Radar -> Seriema

Endpoints:

- `POST /integrations/oasis-radar/pull`
- worker periodico: `oasis_radar_pull_worker`

Variaveis principais:

```bash
OASIS_RADAR_ENABLED=true
OASIS_RADAR_PULL_ENABLED=true
OASIS_RADAR_LOKI_URL=http://loki:3100
OASIS_RADAR_LOGQL_QUERY={compose_service=~".+"}
OASIS_RADAR_LOOKBACK_SECONDS=300
OASIS_RADAR_PULL_LIMIT=100
OASIS_RADAR_PULL_INTERVAL_SECONDS=60
OASIS_RADAR_CURSOR_KEY=integrations:oasis_radar:last_timestamp_ns
OASIS_RADAR_PULL_FAILURES_KEY=integrations:oasis_radar:pull_failures
OASIS_RADAR_PULL_FAILURE_ALERT_THRESHOLD=3
```

Runbook curto:

1. Verifique `GET /health/integrations` e confirme `oasis-radar: ok`.
2. Execute `POST /integrations/oasis-radar/pull` manualmente para validar ingestao.
3. Confira `GET /metrics/ops` e valide chaves:
   `oasis_radar_pull_success_total`, `oasis_radar_pull_failed_total`,
   `oasis_radar_entries_fetched_total`, `oasis_radar_entries_ingested_total`.
4. Se houver falha repetida, revisar `OASIS_RADAR_LOKI_URL` e `OASIS_RADAR_LOGQL_QUERY`.
5. Em incidente de duplicacao excessiva, revisar cursor:
   `OASIS_RADAR_CURSOR_KEY` (Redis).

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

## Observability adicional

A Seriema não precisa receber webhooks públicos de Sentry ou Langfuse.
A arquitetura correta é:

- Sentry -> Loki
- Langfuse -> Loki
- Seriema -> pull no Loki via `oasis_radar_pull_worker`

Variável relacionada:

- `TELEGRAM_NOTIFICATION_DEDUPE_WINDOW_SECONDS` (fallback de throttle para Telegram quando a regra não definir `dedupe_window_seconds`)

## Docs Relacionados

- [Runbook Operacional](plans/runbook_ops.md)
- [Release Checklist](plans/release_checklist.md)
- [Production Handoff](plans/production_handoff.md)
- [Architecture Overview](plans/architecture_overview.md)
