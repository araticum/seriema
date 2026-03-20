# Checklist de Integracao A+B

## Contrato de Dados

- [x] `Incident.status` usa enum padronizado.
- [x] `Notification.status` usa enum padronizado.
- [x] `AuditLog.action` segue lista oficial de acoes.
- [x] `fallback_policy_json` segue chave acordada (`escalation_group_id` e `channels`).

## Contrato de Fluxo

- [x] `trace_id` nasce na ingestao e e propagado ao worker/callback.
- [x] Evento duplicado gera `DUPLICATED_EVENT`.
- [x] ACK por voz atualiza incidente e notificacao exatamente uma vez.
- [x] Escalonamento so roda quando incidente ainda esta `OPEN`.

## Confiabilidade

- [x] Retry/backoff configurado por canal.
- [x] Idempotencia ativa em dispatch e escalonamento.
- [x] Falhas permanentes vao para DLQ (quando implementado).

## Testes Minimos de Integracao

- [x] Ingestao com regra -> notificacao criada -> task roteada.
- [x] Callback de voz com `Digits=1` -> incidente ACK.
- [x] Deadline sem ACK -> incidente ESCALATED + fallback enviado.
- [x] Reprocessamento da mesma task nao duplica notificacoes.
