PY ?= .venv/bin/python
WIN_PY ?= .venv/Scripts/python.exe

ifeq ($(OS),Windows_NT)
PY := $(WIN_PY)
endif

.PHONY: install lock test lint typecheck format-check ci precommit-install

install:
	$(PY) -m pip install -r requirements.txt
	$(PY) -m pip install -r requirements-dev.txt

lock:
	$(PY) -m piptools compile requirements-dev.in -o requirements-dev.txt

test:
	$(PY) -m pytest tests -q

lint:
	$(PY) -m ruff check .

typecheck:
	$(PY) -m pyright .

format-check:
	$(PY) -m ruff format --check .

ci:
	$(PY) -m compileall alembic config.py database.py engine.py main.py models.py redis_client.py schemas.py worker.py tests
	$(PY) -m pytest tests -q
	$(PY) -m alembic -c alembic.ini upgrade head --sql

precommit-install:
	$(PY) -m pre_commit install
