#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV="$ROOT/.venv"
PYTHON="$VENV/bin/python"
PYTEST="$VENV/bin/pytest"
PYRIGHT="${PYRIGHT:-$(command -v pyright || true)}"
RUFF="${RUFF:-$(command -v ruff || true)}"

if [[ ! -x "$PYTHON" ]]; then
  echo "Erro: venv esperado em $VENV, mas $PYTHON não está executável." >&2
  exit 1
fi
if [[ -z "$PYRIGHT" || -z "$RUFF" ]]; then
  echo "Erro: pyright/ruff globais não encontrados no PATH." >&2
  exit 1
fi

export DATABASE_URL="${DATABASE_URL:-postgresql://postgres:postgres@localhost:55432/eventsaas}"
export REDIS_URL="${REDIS_URL:-redis://localhost:56379/0}"
export SERIEMA_DB_SCHEMA="${SERIEMA_DB_SCHEMA:-seriema}"
export SERIEMA_REDIS_DB="${SERIEMA_REDIS_DB:-5}"

cd "$ROOT"

echo ">>> Python: $PYTHON"
"$PYTHON" -V

echo ">>> Ruff"
"$RUFF" check .
"$RUFF" format --check .

echo ">>> Pyright"
"$PYRIGHT" .

for url in "$DATABASE_URL" "$REDIS_URL"; do :; done
if ! command -v psql >/dev/null 2>&1; then
  echo "Aviso: psql não encontrado; pulando checagem prévia do Postgres." >&2
else
  if ! psql "$DATABASE_URL" -c 'select 1' >/dev/null 2>&1; then
    cat >&2 <<EOF
Erro: Postgres de teste indisponível em $DATABASE_URL
Suba a infra local do repo antes do pytest:
  docker compose up -d postgres redis
EOF
    exit 1
  fi
fi

echo ">>> Pytest"
"$PYTEST" tests -q
