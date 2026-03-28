# Testes locais

## Ambiente correto

- backend Python: `./.venv`
- script padronizado: `./scripts/test-local.sh`
- o ambiente local deve ser sempre reconstruído/sincronizado a partir dos arquivos versionados do remoto (`requirements.txt`, `requirements-dev.txt`, scripts e docs)

## Sincronizar com o remoto antes de testar

Sempre que atualizar o clone local, rode este fluxo:

```bash
git fetch origin
# troque main pela branch desejada, se necessário
git pull --ff-only origin main
rm -rf .venv
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
```

Se você quiser apenas reidratar o ambiente sem recriar do zero:

```bash
./.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
```

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

O script usa o `.venv` local do repo e roda:

1. `ruff check .`
2. `ruff format --check .`
3. `pyright .`
4. `pytest tests -q`

## Escopo atual do Pyright

O gate de tipos local foi reduzido de propósito para o conjunto que hoje já consegue gerar sinal útil sem explodir em dezenas de erros legados do repo:

- incluídos: `classifier.py`, `config.py`, `database.py`, `models.py`, `redis_client.py`, `schemas.py`
- fora do gate por enquanto: `main.py`, `worker.py`, `observability.py`, `tests/`, `plans/`, `alembic/`

Isso não "resolve" o backlog de typing desses módulos maiores; só evita transformar o pyright em ruído. O próximo passo recomendado é abrir esse escopo aos poucos, começando por `observability.py`/`main.py` e depois `worker.py`.

## Worker / smoke manual

Se quiser subir a stack completa do repo:

```bash
docker compose up -d
```

Mas para o smoke de testes automatizados locais, `postgres` e `redis` já bastam.

## Artefato ambíguo documentado

Existem scripts PowerShell (`scripts/pre_release_check.ps1`, `scripts/smoke_e2e_handoff.ps1`) focados em Windows/handoff. O caminho Linux padronizado para smoke local deste repo passa a ser `./scripts/test-local.sh`.
