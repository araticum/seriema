import importlib
import json
import sys
import uuid
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
PARENT = ROOT.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))


def _fresh_worker(
    monkeypatch,
    *,
    prefix: str,
    dry_run: str = "true",
    dlq_max_items: str = "1000",
):
    monkeypatch.setenv("SERIEMA_REDIS_KEY_PREFIX", prefix)
    monkeypatch.setenv("SERIEMA_QUEUE_PREFIX", f"queue:{prefix}")
    monkeypatch.setenv("SERIEMA_DLQ_QUEUE_NAME", "dlq")
    monkeypatch.setenv("SERIEMA_DLQ_REPLAY_DRY_RUN", dry_run)
    monkeypatch.setenv("SERIEMA_DLQ_REPLAY_BATCH_SIZE", "10")
    monkeypatch.setenv("SERIEMA_DLQ_MAX_ITEMS", dlq_max_items)
    monkeypatch.setenv("SERIEMA_OPS_MAX_LIMIT", "100")
    monkeypatch.setenv("SERIEMA_METRICS_KEY", "metrics:ops")
    monkeypatch.setenv("SERIEMA_METRICS_TTL_SECONDS", "120")
    monkeypatch.setenv("SERIEMA_METRICS_SNAPSHOT_INTERVAL_SECONDS", "60")
    monkeypatch.setenv("SERIEMA_DLQ_REPLAY_INTERVAL_SECONDS", "120")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:56379/0")
    monkeypatch.setenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:55432/eventsaas")
    monkeypatch.setenv("SERIEMA_DB_SCHEMA", "seriema")

    for name in ["Seriema.config", "Seriema.redis_client", "Seriema.worker"]:
        sys.modules.pop(name, None)

    config = importlib.import_module("Seriema.config")
    redis_client = importlib.import_module("Seriema.redis_client")
    worker = importlib.import_module("Seriema.worker")
    return config, redis_client, worker


def _clear_prefix(redis_conn, prefix: str) -> None:
    keys = redis_conn.keys(f"{prefix}*")
    if keys:
        redis_conn.delete(*keys)


def test_queue_metrics_snapshot(monkeypatch):
    prefix = f"pytest:{uuid.uuid4().hex}"
    _, redis_client, worker = _fresh_worker(monkeypatch, prefix=prefix, dry_run="true")

    _clear_prefix(redis_client.redis_conn, prefix)

    redis_client.redis_conn.delete(worker.DLQ_REDIS_KEY)
    redis_client.redis_conn.rpush(worker.queue_name("dispatch"), "d1", "d2")
    redis_client.redis_conn.rpush(worker.queue_name("voice"), "v1")
    redis_client.redis_conn.rpush(worker.queue_name("telegram"), "t1", "t2", "t3")
    redis_client.redis_conn.rpush(worker.DLQ_REDIS_KEY, json.dumps({"task_name": "voice_worker"}))

    result = worker.queue_metrics_snapshot()

    assert result["queue_backlog:dispatch"] == 2
    assert result["queue_backlog:voice"] == 1
    assert result["queue_backlog:telegram"] == 3
    assert result["queue_backlog:email"] == 0
    assert result["queue_backlog:escalation"] == 0
    assert result["queue_backlog:dlq"] == 1

    metrics = redis_client.redis_conn.hgetall(worker.METRICS_REDIS_KEY)
    assert metrics[b"queue_backlog:dispatch"] == b"2"
    assert metrics[b"queue_backlog:voice"] == b"1"
    assert metrics[b"queue_backlog:telegram"] == b"3"
    assert metrics[b"queue_backlog:dlq"] == b"1"
    assert b"updated_at" in metrics


def test_replay_dlq_dry_run_does_not_remove_items(monkeypatch):
    prefix = f"pytest:{uuid.uuid4().hex}"
    _, redis_client, worker = _fresh_worker(monkeypatch, prefix=prefix, dry_run="true")

    _clear_prefix(redis_client.redis_conn, prefix)

    payload = {
        "task_name": "email_worker",
        "args": ["notification-1", "trace-1"],
        "kwargs": {},
        "trace_id": "trace-1",
        "notification_id": "notification-1",
        "incident_id": "incident-1",
        "error": "boom",
        "failed_at": "fixed",
    }
    redis_client.redis_conn.rpush(worker.DLQ_REDIS_KEY, json.dumps(payload))

    before = redis_client.redis_conn.llen(worker.DLQ_REDIS_KEY)
    result = worker.replay_dlq(limit=2)
    after = redis_client.redis_conn.llen(worker.DLQ_REDIS_KEY)

    assert before == 1
    assert after == 1
    assert result["dry_run"] is True
    assert result["replayed"] == 0
    assert result["remaining"] == 1
    assert result["limit"] == 2
    assert result["candidates"] and result["candidates"][0]["notification_id"] == "notification-1"


def test_replay_dlq_ignores_invalid_entry_without_breaking(monkeypatch):
    prefix = f"pytest:{uuid.uuid4().hex}"
    _, redis_client, worker = _fresh_worker(monkeypatch, prefix=prefix, dry_run="false")

    _clear_prefix(redis_client.redis_conn, prefix)

    redis_client.redis_conn.rpush(worker.DLQ_REDIS_KEY, "{not-json")

    before = redis_client.redis_conn.llen(worker.DLQ_REDIS_KEY)
    result = worker.replay_dlq(limit=1)
    after = redis_client.redis_conn.llen(worker.DLQ_REDIS_KEY)

    assert before == 1
    assert after == 1
    assert result["replayed"] == 0
    assert result["remaining"] == 1


def test_dlq_truncates_and_prunes_to_max_items(monkeypatch):
    prefix = f"pytest:{uuid.uuid4().hex}"
    _, redis_client, worker = _fresh_worker(
        monkeypatch,
        prefix=prefix,
        dry_run="true",
        dlq_max_items="2",
    )

    _clear_prefix(redis_client.redis_conn, prefix)

    for idx in range(3):
        payload = {
            "task_name": "voice_worker",
            "args": [f"notification-{idx}", f"trace-{idx}"],
            "kwargs": {},
            "trace_id": f"trace-{idx}",
            "notification_id": f"notification-{idx}",
            "incident_id": f"incident-{idx}",
            "error": f"boom-{idx}",
            "failed_at": f"fixed-{idx}",
        }
        redis_client.redis_conn.rpush(worker.DLQ_REDIS_KEY, json.dumps(payload))
        worker._trim_dlq_to_limit()

    assert redis_client.redis_conn.llen(worker.DLQ_REDIS_KEY) == 2
    entries = [
        json.loads(item)
        for item in redis_client.redis_conn.lrange(worker.DLQ_REDIS_KEY, 0, -1)
    ]
    assert [entry["notification_id"] for entry in entries] == [
        "notification-1",
        "notification-2",
    ]

    result = worker.prune_dlq(max_items=1)
    assert result["removed"] == 1
    assert result["remaining"] == 1
    assert result["max_items"] == 1
    assert redis_client.redis_conn.llen(worker.DLQ_REDIS_KEY) == 1
