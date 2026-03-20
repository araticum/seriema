# Backlog - Agente A (Plataforma API/Dados)

## P0

1. Introduzir Alembic e migracao inicial do schema.
2. Converter campos de status/channel/action para enums controlados.
3. Adicionar unicidade de incidente em (`source`, `external_event_id`).
4. Definir contratos de resposta para incidente e audit logs.
5. Padronizar auditoria no fluxo HTTP (`EVENT_RECEIVED`, `RULE_MATCHED`, `DUPLICATED_EVENT`, etc.).

## P1

1. Implementar CRUD de `groups`.
2. Implementar CRUD de `group_members`.
3. Implementar `active` e `priority` em `Rule` para ordem de matching.
4. Validar `fallback_policy_json` em schema.

## Criterios de Pronto

- Migration executa sem erro em banco vazio.
- Endpoints com validacao de entrada e saida.
- Campos de estado nao aceitam valores fora do enum.
- Regressao basica de ingestao e consulta de incidente validada.
