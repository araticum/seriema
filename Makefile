PY=.venv/Scripts/python.exe

.PHONY: install lock test lint ci precommit-install

install:
	$(PY) -m pip install -r requirements.txt
	$(PY) -m pip install -r requirements-dev.txt

lock:
	$(PY) -m piptools compile requirements-dev.in -o requirements-dev.txt

test:
	$(PY) -m pytest tests -q

lint:
	$(PY) -m ruff check .
	$(PY) -m black --check .

ci:
	$(PY) -m compileall alembic config.py database.py engine.py main.py models.py redis_client.py schemas.py worker.py tests
	$(PY) -m pytest tests -q
	$(PY) -m alembic -c alembic.ini upgrade head --sql

precommit-install:
	$(PY) -m pre_commit install
