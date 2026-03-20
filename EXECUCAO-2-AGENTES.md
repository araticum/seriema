# Execucao Paralela com 2 Agentes

Este documento implementa a divisao de trabalho em duas trilhas com baixo acoplamento para acelerar a construcao do sistema.

## Objetivo

Separar em:
- **Agente A (Plataforma API/Dados)**: camada transacional e contratos.
- **Agente B (Execucao Assincrona/Notificacao)**: fila, workers e confiabilidade operacional.

## Ownership por Arquivo (estado atual)

### Agente A - Plataforma API/Dados
- `database.py`
- `models.py`
- `schemas.py`
- `engine.py`
- `main.py` (somente endpoints CRUD, ingestao, resposta e auditoria de API)

### Agente B - Assincrono/Notificacao
- `worker.py`
- `redis_client.py`
- `main.py` (somente callbacks e pontos de integracao com workers)

## Regra de Convivencia para `main.py`

Como `main.py` e arquivo compartilhado:
- Agente A altera apenas:
  - `/events/incoming`
  - CRUD de `rules`, `contacts`, `groups`, `group_members`
  - `/incidents/{id}`
  - serializacao de resposta e `trace_id` no fluxo HTTP
- Agente B altera apenas:
  - `/dispatch/voice/twiml/{notification_id}`
  - `/dispatch/voice/callback/{notification_id}`
  - futuros callbacks de canal (`telegram`, `email`, etc.)

## Contrato de Integracao entre A e B

Agente A entrega e estabiliza:
1. Schema e constraints (incluindo enums e unicidade de incidente).
2. Estados validos de `Incident` e `Notification`.
3. Estrutura e nomenclatura de `AuditLog.action`.
4. Campos obrigatorios de `fallback_policy_json`.

Agente B consome e respeita:
1. Estados e transicoes definidos por A.
2. Padrao de `trace_id` para logs de worker.
3. Contrato de fallback/escalonamento definido por A.

## Sequencia Recomendada (sem bloqueio forte)

1. A implementa migracoes + constraints + contratos de schema.
2. B implementa roteamento de filas + idempotencia + retries.
3. A finaliza endpoints faltantes de grupos e membros.
4. B fecha ciclo de ACK/escalonamento com status consistentes.
5. Integracao conjunta e testes de ponta a ponta.

## Definition of Done por Trilha

### A pronto quando:
- `alembic upgrade head` funciona em ambiente limpo.
- API responde com schemas estaveis para incidentes/logs.
- CRUD de grupos/membros e regras completo.
- Acoes de auditoria padronizadas.

### B pronto quando:
- Filas por canal estao roteadas.
- Retries/idempotencia evitam duplicidade.
- ACK atualiza incidente/notificacao com consistencia.
- Escalonamento respeita timeout e fallback.

## Checklist de Integracao

Use o checklist em `plans/integration-checklist.md` antes de merge final.
