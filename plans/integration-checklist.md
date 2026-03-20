# Checklist de Integracao A+B

## Contrato de Dados

- [ ] `Incident.status` usa enum padronizado.
- [ ] `Notification.status` usa enum padronizado.
- [ ] `AuditLog.action` segue lista oficial de acoes.
- [ ] `fallback_policy_json` segue chave acordada (`escalation_group_id` e `channels`).

## Contrato de Fluxo

- [ ] `trace_id` nasce na ingestao e e propagado ao worker/callback.
- [ ] Evento duplicado gera `DUPLICATED_EVENT`.
- [ ] ACK por voz atualiza incidente e notificacao exatamente uma vez.
- [ ] Escalonamento so roda quando incidente ainda esta `OPEN`.

## Confiabilidade

- [ ] Retry/backoff configurado por canal.
- [ ] Idempotencia ativa em dispatch e escalonamento.
- [ ] Falhas permanentes vao para DLQ (quando implementado).

## Testes Minimos de Integracao

- [ ] Ingestao com regra -> notificacao criada -> task roteada.
- [ ] Callback de voz com `Digits=1` -> incidente ACK.
- [ ] Deadline sem ACK -> incidente ESCALATED + fallback enviado.
- [ ] Reprocessamento da mesma task nao duplica notificacoes.
