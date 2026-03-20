# Runbook Operacional

## Objetivo

Manter o ambiente da Seriema estável quando houver crescimento de DLQ, falhas de callback, backlog de filas ou necessidade de rollback.

## Variáveis Importantes

- `SERIEMA_OPS_MAX_LIMIT`: limite máximo aceito em endpoints operacionais.
- `VOICE_WEBHOOK_MAX_AGE_SECONDS`: janela máxima de validade de assinatura do callback de voz.
- `SERIEMA_DLQ_REPLAY_DRY_RUN`: quando `true`, replay deve ser tratado como simulação operacional.
- `SERIEMA_DLQ_REPLAY_BATCH_SIZE`: lote padrão para replay.

## Quando a DLQ crescer

1. Verifique o crescimento atual.
2. Identifique o tipo de falha principal pelos campos `task_name` e `error`.
3. Confirme se a causa é transitória ou permanente.
4. Se for transitória, prepare replay por lote pequeno.
5. Se for permanente, corrija a causa antes de reenfileirar.

Sinais de alerta:
- subida contínua por mais de 10 minutos
- a mesma tarefa aparece repetidamente
- aumento de `FAILED` sem queda em `SENT`

## Replay Seguro

1. Faça preview da DLQ antes de reenfileirar.
2. Execute replay em lote pequeno primeiro.
3. Observe `FAILED`, `SENT` e backlog das filas após o replay.
4. Se `SERIEMA_DLQ_REPLAY_DRY_RUN=true`, trate a operação como teste e não como recuperação real.
5. Nunca reenfileire uma DLQ inteira sem checar a causa original.

## Verificacao De Filas

1. Consulte o snapshot operacional de filas.
2. Compare `dispatch`, `voice`, `telegram`, `email`, `escalation` e `dlq`.
3. Se `dispatch` crescer, verifique ingestão e matching de regras.
4. Se `voice` crescer, verifique credenciais, callback e tempo de resposta do provedor.
5. Se `dlq` crescer, abra triagem antes de continuar expandindo replay.

## Rollback De Migracao

1. Confirme a revisão aplicada.
2. Verifique se a migração alterou apenas schema compatível.
3. Se houver problema após deploy, volte uma revisão por vez.
4. Valide a aplicação depois do rollback com `health` e consulta de schema.
5. Nunca faça rollback cego se houver perda de dados potencial.

## Checklist Pre-Deploy

- Confirmar que `alembic upgrade head` funciona em banco limpo.
- Confirmar que a fila `dlq` está vazia ou triada.
- Confirmar que o snapshot operacional está atualizando.
- Confirmar que `/ops/integration/status` e `/ops/readiness` retornam sinais coerentes.
- Confirmar que o webhook de voz usa a janela de assinatura esperada.
- Confirmar que o limite operacional dos endpoints está configurado.

## Checklist Pos-Deploy

- Validar `health`.
- Validar ingestão de evento teste.
- Validar callback de voz em ambiente de teste.
- Validar `metrics/sla`.
- Validar `metrics/queues`.
- Validar preview e replay de DLQ com token operacional, se configurado.
- Validar `/ops/dlq/replay/last` e `/ops/integration/status`.

## Smoke Final De Handoff

Use este comando para o checklist final antes do aceite operacional:

`powershell -ExecutionPolicy Bypass -File .\scripts\smoke_e2e_handoff.ps1`

O smoke percorre `health`, `health/deps`, `metrics` basicos, `ops/integration/status`, `ops/readiness` e, por fim, faz fallback para `pre_release_check`.

Se a API nao estiver disponivel, o smoke deve registrar a falha de conexao nas etapas HTTP e ainda executar o fallback local para deixar o estado do pipeline visivel.
