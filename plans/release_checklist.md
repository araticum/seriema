# Release Checklist

## Antes do Deploy

- [ ] `alembic upgrade head` aplica sem erro no ambiente alvo.
- [ ] `GET /health` responde `200`.
- [ ] `GET /metrics/sla`, `GET /metrics/queues`, `GET /ops/integration/status` e `GET /ops/readiness` retornam payloads válidos.
- [ ] Fila `dispatch`, `voice`, `telegram`, `email`, `escalation` e `DLQ` estão com backlog esperado.
- [ ] `SERIEMA_ADMIN_TOKEN` e `VOICE_WEBHOOK_SECRET` estão configurados quando exigido.
- [ ] `powershell -ExecutionPolicy Bypass -File .\scripts\smoke_e2e_handoff.ps1` executa e o resumo final fica arquivado para o handoff.

## Durante o Deploy

- [ ] Monitorar logs de `FAILED`, `ESCALATED` e `ACK_RECEIVED`.
- [ ] Confirmar que workers sobem com `beat_schedule` ativo.
- [ ] Validar replay DLQ em modo `dry-run` antes de replay real.

## Depois do Deploy

- [ ] Validar ingestao de evento de teste fim a fim.
- [ ] Confirmar que callback autenticado de voz funciona.
- [ ] Verificar dashboards do Metabase nas views `v_incident_sla`, `v_channel_delivery` e `v_ops_summary_24h`.
- [ ] Revisar backlog da DLQ e repetir replay apenas se for seguro.
- [ ] Confirmar que `/ops/dlq/replay/last` reflete o ultimo replay.
- [ ] Confirmar que `/ops/integration/status` e `/ops/readiness` estao consistentes antes do go-live.
- [ ] Se a API nao estiver disponivel no ambiente local, o smoke deve registrar isso e cair para `pre_release_check` com falha explicita nas etapas HTTP.

## Rollback

- [ ] Reverter a ultima migration se o problema estiver no schema.
- [ ] Desabilitar consumidores de worker antes de corrigir dados corrompidos.
- [ ] Preservar a DLQ para analise antes de qualquer limpeza.
