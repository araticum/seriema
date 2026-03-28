# Testes locais

## Ambiente correto

- backend Python: `./.venv`
- script padronizado: `./scripts/test-local.sh`
- ferramentas globais esperadas para qualidade: `ruff` e `pyright`

## Infra local para testes

Os testes e smoke locais assumem a mesma infra documentada no CI:

```bash
docker compose up -d postgres redis
```

Portas/valores locais esperados:

- Postgres: `postgresql://postgres:postgres@localhost:55432/eventsaas`
- Redis: `redis://localhost:56379/0`
- schema: `seriema`

## Rodando qualidade + testes

```bash
./scripts/test-local.sh
```

O script padronizado roda:

1. `ruff check .`
2. `ruff format --check .`
3. `pyright .`
4. `pytest tests -q`

## Worker / smoke manual

Se quiser subir a stack completa do repo:

```bash
docker compose up -d
```

Mas para o smoke de testes automatizados locais, `postgres` e `redis` já bastam.

## Artefato ambíguo documentado

Existem scripts PowerShell (`scripts/pre_release_check.ps1`, `scripts/smoke_e2e_handoff.ps1`) focados em Windows/handoff. O caminho Linux padronizado para smoke local deste repo passa a ser `./scripts/test-local.sh`.
