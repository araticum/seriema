import importlib
import json
from enum import Enum
import fnmatch
from datetime import datetime, timedelta, timezone
import sys
import uuid
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
PARENT = ROOT.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))


class _FakePipeline:
    def __init__(self, redis_conn):
        self._redis_conn = redis_conn
        self._ops = []

    def set(self, *args, **kwargs):
        self._ops.append(("set", args, kwargs))
        return self

    def delete(self, *args, **kwargs):
        self._ops.append(("delete", args, kwargs))
        return self

    def hset(self, *args, **kwargs):
        self._ops.append(("hset", args, kwargs))
        return self

    def hincrby(self, *args, **kwargs):
        self._ops.append(("hincrby", args, kwargs))
        return self

    def expire(self, *args, **kwargs):
        self._ops.append(("expire", args, kwargs))
        return self

    def execute(self):
        results = []
        for op_name, args, kwargs in self._ops:
            results.append(getattr(self._redis_conn, op_name)(*args, **kwargs))
        self._ops.clear()
        return results


class _FakeRedis:
    def __init__(self):
        self._values = {}

    @staticmethod
    def _normalize(value):
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, uuid.UUID):
            return str(value)
        return value

    @staticmethod
    def _encode(value):
        value = _FakeRedis._normalize(value)
        if value is None:
            return None
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return value.encode("utf-8")
        return str(value).encode("utf-8")

    def keys(self, pattern):
        return [key for key in self._values if fnmatch.fnmatch(key, pattern)]

    def delete(self, *keys):
        removed = 0
        for key in keys:
            key = key.decode("utf-8") if isinstance(key, bytes) else key
            if key in self._values:
                del self._values[key]
                removed += 1
        return removed

    def exists(self, key):
        key = key.decode("utf-8") if isinstance(key, bytes) else key
        return 1 if key in self._values else 0

    def get(self, key):
        key = key.decode("utf-8") if isinstance(key, bytes) else key
        value = self._values.get(key)
        if value is None:
            return None
        if isinstance(value, dict):
            return None
        if isinstance(value, list):
            return None
        return self._encode(value)

    def set(self, key, value, nx=False, ex=None):
        key = key.decode("utf-8") if isinstance(key, bytes) else key
        if nx and key in self._values:
            return False
        self._values[key] = self._normalize(value)
        return True

    def incr(self, key):
        key = key.decode("utf-8") if isinstance(key, bytes) else key
        current = self._values.get(key, 0)
        if isinstance(current, bytes):
            current = current.decode("utf-8")
        if isinstance(current, str):
            current = int(current)
        if current is None:
            current = 0
        current = int(current) + 1
        self._values[key] = current
        return current

    def expire(self, key, seconds):
        return True

    def rpush(self, key, *values):
        key = key.decode("utf-8") if isinstance(key, bytes) else key
        bucket = self._values.setdefault(key, [])
        if not isinstance(bucket, list):
            raise TypeError(f"Key {key} is not a list")
        bucket.extend(self._normalize(value) for value in values)
        return len(bucket)

    def llen(self, key):
        key = key.decode("utf-8") if isinstance(key, bytes) else key
        bucket = self._values.get(key, [])
        return len(bucket) if isinstance(bucket, list) else 0

    def lrange(self, key, start, end):
        key = key.decode("utf-8") if isinstance(key, bytes) else key
        bucket = self._values.get(key, [])
        if not isinstance(bucket, list):
            return []
        items = bucket[:]
        if end == -1:
            end = len(items) - 1
        if start < 0:
            start = len(items) + start
        if end < 0:
            end = len(items) + end
        if end < start:
            return []
        return items[start : end + 1]

    def ltrim(self, key, start, end):
        key = key.decode("utf-8") if isinstance(key, bytes) else key
        bucket = self._values.get(key, [])
        if not isinstance(bucket, list):
            return True
        items = bucket[:]
        if end == -1:
            end = len(items) - 1
        if start < 0:
            start = len(items) + start
        if end < 0:
            end = len(items) + end
        if end < start:
            self._values[key] = []
            return True
        self._values[key] = items[start : end + 1]
        return True

    def lrem(self, key, count, value):
        key = key.decode("utf-8") if isinstance(key, bytes) else key
        bucket = self._values.get(key, [])
        if not isinstance(bucket, list):
            return 0
        target = self._normalize(value)
        removed = 0
        remaining = []
        for item in bucket:
            if item == target and (count == 0 or removed < abs(count)):
                removed += 1
                continue
            remaining.append(item)
        self._values[key] = remaining
        return removed

    def hset(self, key, field=None, value=None, mapping=None):
        key = key.decode("utf-8") if isinstance(key, bytes) else key
        bucket = self._values.setdefault(key, {})
        if not isinstance(bucket, dict):
            raise TypeError(f"Key {key} is not a hash")
        if mapping is not None:
            for map_key, map_value in mapping.items():
                bucket[str(map_key)] = self._normalize(map_value)
            return True
        if field is not None:
            bucket[str(field)] = self._normalize(value)
            return True
        return False

    def hincrby(self, key, field, amount=1):
        key = key.decode("utf-8") if isinstance(key, bytes) else key
        bucket = self._values.setdefault(key, {})
        if not isinstance(bucket, dict):
            raise TypeError(f"Key {key} is not a hash")
        current = bucket.get(str(field), 0)
        if isinstance(current, bytes):
            current = current.decode("utf-8")
        if isinstance(current, str):
            current = int(current)
        current = int(current) + int(amount)
        bucket[str(field)] = current
        return current

    def hgetall(self, key):
        key = key.decode("utf-8") if isinstance(key, bytes) else key
        bucket = self._values.get(key, {})
        if not isinstance(bucket, dict):
            return {}
        return {
            str(field).encode("utf-8"): self._encode(value)
            for field, value in bucket.items()
        }

    def pipeline(self):
        return _FakePipeline(self)


class _FakeQuery:
    def __init__(self, store, model):
        self._store = store
        self._model = model
        self._conditions = []

    @staticmethod
    def _normalize(value):
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, uuid.UUID):
            return str(value)
        return value

    @staticmethod
    def _extract_expression_value(expr):
        right = getattr(expr, "right", None)
        if hasattr(right, "value"):
            return right.value
        return right

    def filter(self, *conditions):
        self._conditions.extend(conditions)
        return self

    def with_for_update(self):
        return self

    def _matches(self, obj):
        for condition in self._conditions:
            left = getattr(condition, "left", None)
            right = self._extract_expression_value(condition)
            key = getattr(left, "key", None) or getattr(left, "name", None)
            if key is None:
                continue
            obj_value = getattr(obj, key)
            operator_name = getattr(getattr(condition, "operator", None), "__name__", None)
            if operator_name in {"eq", None}:
                if self._normalize(obj_value) != self._normalize(right):
                    return False
                continue
            if operator_name == "lt":
                if not (obj_value < right):
                    return False
                continue
            if operator_name == "le":
                if not (obj_value <= right):
                    return False
                continue
            if operator_name == "gt":
                if not (obj_value > right):
                    return False
                continue
            if operator_name == "ge":
                if not (obj_value >= right):
                    return False
                continue
        return True

    def all(self):
        return [obj for obj in self._store.get(self._model, []) if self._matches(obj)]

    def first(self):
        items = self.all()
        return items[0] if items else None


class _FakeSession:
    def __init__(self, store):
        self._store = store

    @staticmethod
    def _ensure_id(obj):
        if hasattr(obj, "id") and getattr(obj, "id") is None:
            setattr(obj, "id", uuid.uuid4())

    def add(self, obj):
        self._ensure_id(obj)
        bucket = self._store.setdefault(type(obj), [])
        if obj not in bucket:
            bucket.append(obj)

    def commit(self):
        return None

    def refresh(self, obj):
        return None

    def close(self):
        return None

    def query(self, model):
        return _FakeQuery(self._store, model)


def _fresh_worker(
    monkeypatch,
    *,
    prefix: str,
    dry_run: str = "true",
    dlq_max_items: str = "1000",
    lock_ttl: str = "60",
    cb_failure_threshold: str = "5",
    cb_open_seconds: str = "60",
    rate_limit_per_minute: str = "60",
    rate_limit_window_seconds: str = "60",
):
    monkeypatch.setenv("SERIEMA_REDIS_KEY_PREFIX", prefix)
    monkeypatch.setenv("SERIEMA_QUEUE_PREFIX", f"queue:{prefix}")
    monkeypatch.setenv("SERIEMA_DLQ_QUEUE_NAME", "dlq")
    monkeypatch.setenv("SERIEMA_DLQ_REPLAY_DRY_RUN", dry_run)
    monkeypatch.setenv("SERIEMA_DLQ_REPLAY_BATCH_SIZE", "10")
    monkeypatch.setenv("SERIEMA_DLQ_MAX_ITEMS", dlq_max_items)
    monkeypatch.setenv("SERIEMA_DLQ_REPLAY_LOCK_TTL_SECONDS", lock_ttl)
    monkeypatch.setenv("SERIEMA_CB_FAILURE_THRESHOLD", cb_failure_threshold)
    monkeypatch.setenv("SERIEMA_CB_OPEN_SECONDS", cb_open_seconds)
    monkeypatch.setenv("SERIEMA_CHANNEL_RATE_LIMIT_PER_MINUTE", rate_limit_per_minute)
    monkeypatch.setenv("SERIEMA_CHANNEL_RATE_LIMIT_WINDOW_SECONDS", rate_limit_window_seconds)
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

    fake_redis = _FakeRedis()
    fake_store = {}

    monkeypatch.setattr(redis_client, "redis_conn", fake_redis, raising=False)
    monkeypatch.setattr(worker, "redis_conn", fake_redis, raising=False)
    monkeypatch.setattr(worker, "SessionLocal", lambda: _FakeSession(fake_store), raising=False)
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


def test_replay_dlq_returns_locked_when_lock_is_held(monkeypatch):
    prefix = f"pytest:{uuid.uuid4().hex}"
    _, redis_client, worker = _fresh_worker(monkeypatch, prefix=prefix, dry_run="false")

    _clear_prefix(redis_client.redis_conn, prefix)

    redis_client.redis_conn.set(worker.DLQ_REPLAY_LOCK_KEY, "other-token", ex=60)
    redis_client.redis_conn.rpush(
        worker.DLQ_REDIS_KEY,
        json.dumps(
            {
                "task_name": "voice_worker",
                "args": ["notification-locked", "trace-locked"],
                "kwargs": {},
                "trace_id": "trace-locked",
                "notification_id": "notification-locked",
                "incident_id": "incident-locked",
                "error": "boom",
                "failed_at": "fixed-locked",
            }
        ),
    )

    result = worker.replay_dlq(limit=1)

    assert result["status"] == "locked"
    assert result["replayed"] == 0
    assert redis_client.redis_conn.llen(worker.DLQ_REDIS_KEY) == 1
    assert redis_client.redis_conn.get(worker.DLQ_REPLAY_LOCK_KEY) == b"other-token"


def test_replay_dlq_releases_lock_after_success(monkeypatch):
    prefix = f"pytest:{uuid.uuid4().hex}"
    _, redis_client, worker = _fresh_worker(
        monkeypatch,
        prefix=prefix,
        dry_run="true",
    )

    _clear_prefix(redis_client.redis_conn, prefix)

    redis_client.redis_conn.rpush(
        worker.DLQ_REDIS_KEY,
        json.dumps(
            {
                "task_name": "email_worker",
                "args": ["notification-release", "trace-release"],
                "kwargs": {},
                "trace_id": "trace-release",
                "notification_id": "notification-release",
                "incident_id": "incident-release",
                "error": "boom",
                "failed_at": "fixed-release",
            }
        ),
    )

    result = worker.replay_dlq(limit=1)

    assert result["status"] == "completed"
    assert result["dry_run"] is True
    assert redis_client.redis_conn.get(worker.DLQ_REPLAY_LOCK_KEY) is None


def test_voice_circuit_opens_and_skips_while_open(monkeypatch):
    prefix = f"pytest:{uuid.uuid4().hex}"
    _, redis_client, worker = _fresh_worker(
        monkeypatch,
        prefix=prefix,
        dry_run="true",
        cb_failure_threshold="2",
        cb_open_seconds="60",
    )

    _clear_prefix(redis_client.redis_conn, prefix)

    first = worker._record_channel_failure(worker.NotificationChannel.VOICE)
    second = worker._record_channel_failure(worker.NotificationChannel.VOICE)

    assert first["opened"] is False
    assert second["opened"] is True
    assert redis_client.redis_conn.exists(worker._channel_circuit_open_key("VOICE")) == 1

    from Seriema import models

    db = worker.SessionLocal()
    try:
        contact = models.Contact(name="Circuit Open", email="circuit@example.com")
        incident = models.Incident(
            external_event_id=f"evt-{uuid.uuid4().hex}",
            source="pytest",
            severity="LOW",
            title="circuit",
            status=worker.IncidentStatus.OPEN,
        )
        db.add(contact)
        db.add(incident)
        db.commit()
        db.refresh(contact)
        db.refresh(incident)

        notification = models.Notification(
            incident_id=incident.id,
            contact_id=contact.id,
            channel=worker.NotificationChannel.VOICE,
        )
        db.add(notification)
        db.commit()
        db.refresh(notification)
    finally:
        db.close()

    def _fail_if_called(*args, **kwargs):
        raise AssertionError("mark_notification_sent should not be called while circuit is open")

    monkeypatch.setattr(worker, "_mark_notification_sent", _fail_if_called)
    result = worker._send_voice_call_impl(str(notification.id), "trace-circuit")

    assert result["status"] == "skipped_circuit"
    assert result["channel"] == "VOICE"
    assert result["notification_id"] == str(notification.id)


def test_success_resets_channel_failure_counter(monkeypatch):
    prefix = f"pytest:{uuid.uuid4().hex}"
    _, redis_client, worker = _fresh_worker(
        monkeypatch,
        prefix=prefix,
        dry_run="true",
        cb_failure_threshold="3",
    )

    _clear_prefix(redis_client.redis_conn, prefix)

    worker._record_channel_failure(worker.NotificationChannel.VOICE)
    assert redis_client.redis_conn.get(worker._channel_circuit_failure_key("VOICE")) == b"1"

    from Seriema import models

    db = worker.SessionLocal()
    try:
        contact = models.Contact(name="Circuit Reset", email="reset@example.com")
        incident = models.Incident(
            external_event_id=f"evt-{uuid.uuid4().hex}",
            source="pytest",
            severity="LOW",
            title="reset",
            status=worker.IncidentStatus.OPEN,
        )
        db.add(contact)
        db.add(incident)
        db.commit()
        db.refresh(contact)
        db.refresh(incident)

        notification = models.Notification(
            incident_id=incident.id,
            contact_id=contact.id,
            channel=worker.NotificationChannel.VOICE,
        )
        db.add(notification)
        db.commit()
        db.refresh(notification)
    finally:
        db.close()

    result = worker._send_voice_call_impl(str(notification.id), "trace-success")

    assert result["status"] == "sent"
    assert redis_client.redis_conn.get(worker._channel_circuit_failure_key("VOICE")) is None


def test_rate_limit_allows_first_send_and_blocks_second(monkeypatch):
    prefix = f"pytest:{uuid.uuid4().hex}"
    _, redis_client, worker = _fresh_worker(
        monkeypatch,
        prefix=prefix,
        dry_run="true",
        rate_limit_per_minute="1",
        rate_limit_window_seconds="60",
    )

    _clear_prefix(redis_client.redis_conn, prefix)

    from Seriema import models

    db = worker.SessionLocal()
    try:
        contact = models.Contact(name="Rate Limit", email="rate@example.com")
        incident = models.Incident(
            external_event_id=f"evt-{uuid.uuid4().hex}",
            source="pytest",
            severity="LOW",
            title="rate limit",
            status=worker.IncidentStatus.OPEN,
        )
        db.add(contact)
        db.add(incident)
        db.commit()
        db.refresh(contact)
        db.refresh(incident)

        first = models.Notification(
            incident_id=incident.id,
            contact_id=contact.id,
            channel=worker.NotificationChannel.VOICE,
        )
        second = models.Notification(
            incident_id=incident.id,
            contact_id=contact.id,
            channel=worker.NotificationChannel.VOICE,
        )
        db.add(first)
        db.add(second)
        db.commit()
        db.refresh(first)
        db.refresh(second)
    finally:
        db.close()

    first_result = worker._send_voice_call_impl(str(first.id), "trace-rate-1")
    second_result = worker._send_voice_call_impl(str(second.id), "trace-rate-2")

    assert first_result["status"] == "sent"
    assert second_result["status"] == "skipped_rate_limit"
    assert second_result["rate_limit"]["limit"] == 1
    assert second_result["rate_limit"]["count"] == 2
    assert redis_client.redis_conn.get(worker._channel_circuit_failure_key("VOICE")) is None


def test_stale_incident_sweeper_escalates_only_old_open_incidents(monkeypatch):
    prefix = f"pytest:{uuid.uuid4().hex}"
    _, redis_client, worker = _fresh_worker(
        monkeypatch,
        prefix=prefix,
        dry_run="true",
    )

    _clear_prefix(redis_client.redis_conn, prefix)

    from Seriema import models

    db = worker.SessionLocal()
    try:
        now = datetime.now(timezone.utc)
        old_open = models.Incident(
            external_event_id=f"evt-{uuid.uuid4().hex}",
            source="pytest",
            severity="LOW",
            title="old-open",
            status=worker.IncidentStatus.OPEN,
            created_at=now - timedelta(minutes=45),
        )
        recent_open = models.Incident(
            external_event_id=f"evt-{uuid.uuid4().hex}",
            source="pytest",
            severity="LOW",
            title="recent-open",
            status=worker.IncidentStatus.OPEN,
            created_at=now - timedelta(minutes=10),
        )
        acknowledged = models.Incident(
            external_event_id=f"evt-{uuid.uuid4().hex}",
            source="pytest",
            severity="LOW",
            title="acknowledged",
            status=worker.IncidentStatus.ACKNOWLEDGED,
            created_at=now - timedelta(minutes=45),
        )
        db.add(old_open)
        db.add(recent_open)
        db.add(acknowledged)
        db.commit()
        db.refresh(old_open)
        db.refresh(recent_open)
        db.refresh(acknowledged)
    finally:
        db.close()

    result = worker.stale_incident_sweeper()

    db = worker.SessionLocal()
    try:
        db.refresh(old_open)
        db.refresh(recent_open)
        db.refresh(acknowledged)
        assert old_open.status == worker.IncidentStatus.ESCALATED
        assert recent_open.status == worker.IncidentStatus.OPEN
        assert acknowledged.status == worker.IncidentStatus.ACKNOWLEDGED

        logs = db.query(models.AuditLog).filter(models.AuditLog.incident_id == old_open.id).all()
        assert any(
            log.action == models.AuditAction.ESCALATED and log.details_json.get("reason") == "stale_sweeper"
            for log in logs
        )
    finally:
        db.close()

    assert result["swept"] == 1
