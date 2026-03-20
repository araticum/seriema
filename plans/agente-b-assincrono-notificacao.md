# Backlog - Agente B (Assincrono/Notificacao)

## P0

1. Definir filas por canal (`dispatch`, `voice`, `telegram`, `email`, `escalation`).
2. Implementar roteamento explicito de tasks por fila.
3. Implementar idempotencia de dispatch/escalonamento para evitar notificacoes duplicadas.
4. Corrigir ciclo de ACK da voz:
   - atualizar `Incident.status`
   - atualizar `Incident.acknowledged_at`
   - atualizar `Notification.status`
5. Corrigir escalonamento para marcar incidente como `ESCALATED` quando aplicavel.

## P1

1. Implementar retry/backoff por canal.
2. Externalizar URLs de callback/TwiML para env vars.
3. Completar auditoria de worker com `trace_id` consistente.
4. Implementar worker stub de Telegram e Email com status funcional.

## P2

1. Implementar DLQ para falhas permanentes.
2. Criar rotina de replay/manual requeue.
3. Expor metricas basicas de fila e tempo de ACK.

## Criterios de Pronto

- Reexecucao de task nao cria duplicidade de notificacao.
- ACK de voz e escalonamento respeitam transicoes de estado.
- Falha transitoria do canal segue politica de retry.
- Fluxo deixa trilha de auditoria consistente por `trace_id`.
