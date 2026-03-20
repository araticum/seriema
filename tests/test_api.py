import os
from dataclasses import dataclass

import pytest
from fastapi.testclient import TestClient

from Seriema import main, models, schemas


class FakeQuery:
    def __init__(self, result):
        self.result = result

    def filter(self, *args, **kwargs):
        return self

    def group_by(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def scalar(self):
        return self.result

    def all(self):
        return self.result

    def first(self):
        return self.result


class FakeSession:
    def __init__(self, queries):
        self._queries = list(queries)
        self.executed = []

    def query(self, *args, **kwargs):
        if not self._queries:
            raise AssertionError("Unexpected query call")
        return FakeQuery(self._queries.pop(0))

    def execute(self, statement):
        self.executed.append(statement)
        class _ScalarResult:
            def scalar(self_inner):
                return 1
        return _ScalarResult()

    def add(self, *args, **kwargs):
        return None

    def commit(self):
        return None

    def rollback(self):
        return None

    def refresh(self, *args, **kwargs):
        return None


@dataclass
class FakeRedis:
    lengths: dict
    dlq_entries: list[bytes]
    hashes: dict | None = None

    def llen(self, key):
        return self.lengths.get(key, len(self.dlq_entries) if key.endswith(":dlq") else 0)

    def lrange(self, key, start, end):
        if key.endswith(":dlq"):
            return self.dlq_entries[start : end + 1]
        return []

    def pipeline(self):
        return self

    def hset(self, *args, **kwargs):
        return None

    def hincrby(self, *args, **kwargs):
        return None

    def expire(self, *args, **kwargs):
        return None

    def execute(self):
        return None

    def rpush(self, key, value):
        if key.endswith(":dlq"):
            self.dlq_entries.append(value.encode("utf-8") if isinstance(value, str) else value)
            return len(self.dlq_entries)
        return 0

    def delete(self, *args, **kwargs):
        self.dlq_entries.clear()
        return None

    def ping(self):
        return True

    def hgetall(self, key):
        return self.hashes.get(key, {}) if self.hashes is not None else {}


class FakeAsyncResult:
    def __init__(self, payload, task_id="fake-task-id"):
        self._payload = payload
        self.id = task_id

    def get(self, propagate=False):
        return self._payload


class FakeReplayTask:
    def __init__(self, payload):
        self.payload = payload

    def apply_async(self, args=None, kwargs=None):
        return FakeAsyncResult(self.payload)


@pytest.fixture(autouse=True)
def reset_overrides():
    main.app.dependency_overrides = {}
    yield
    main.app.dependency_overrides = {}


@pytest.fixture
def client():
    return TestClient(main.app)


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_health_deps_ok(client, monkeypatch):
    fake_session = FakeSession(queries=[])
    fake_redis = FakeRedis(lengths={}, dlq_entries=[])
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    monkeypatch.setattr(main, "redis_conn", fake_redis)

    response = client.get("/health/deps")

    assert response.status_code == 200
    payload = response.json()
    assert payload["postgres"]["status"] == "ok"
    assert payload["redis"]["status"] == "ok"
    assert payload["overall"] == "ok"


def test_health_deps_down(client, monkeypatch):
    class BrokenSession(FakeSession):
        def execute(self, statement):
            raise RuntimeError("postgres unavailable")

    class BrokenRedis(FakeRedis):
        def ping(self):
            raise RuntimeError("redis unavailable")

    main.app.dependency_overrides[main.get_db] = lambda: BrokenSession(queries=[])
    monkeypatch.setattr(main, "redis_conn", BrokenRedis(lengths={}, dlq_entries=[]))

    response = client.get("/health/deps")

    assert response.status_code == 200
    payload = response.json()
    assert payload["postgres"]["status"] == "down"
    assert payload["redis"]["status"] == "down"
    assert payload["overall"] == "down"


def test_metrics_sla_valid(client):
    fake_session = FakeSession(
        queries=[
            3,
            1,
            42.5,
            [("OPEN", 1), ("ACKNOWLEDGED", 1), ("ESCALATED", 1)],
        ]
    )
    main.app.dependency_overrides[main.get_db] = lambda: fake_session

    response = client.get("/metrics/sla?hours=24")

    assert response.status_code == 200
    payload = response.json()
    assert payload["hours"] == 24
    assert payload["total_incidents"] == 3
    assert payload["acknowledged_incidents"] == 1
    assert payload["acknowledgement_rate"] == pytest.approx(1 / 3)
    assert payload["average_tta_seconds"] == pytest.approx(42.5)
    assert [row["status"] for row in payload["incidents_by_status"]] == [
        "OPEN",
        "ACKNOWLEDGED",
        "ESCALATED",
    ]


@pytest.mark.parametrize("hours", [0, 721])
def test_metrics_sla_invalid_hours(client, hours):
    response = client.get(f"/metrics/sla?hours={hours}")
    assert response.status_code == 422


def test_metrics_queues(client, monkeypatch):
    fake_redis = FakeRedis(
        lengths={
            main.queue_name("dispatch"): 4,
            main.queue_name("voice"): 3,
            main.queue_name("telegram"): 2,
            main.queue_name("email"): 1,
            main.queue_name("escalation"): 5,
            main.prefixed_redis_key(main.DLQ_QUEUE_NAME): 6,
        },
        dlq_entries=[],
    )
    monkeypatch.setattr(main, "redis_conn", fake_redis)

    response = client.get("/metrics/queues")
    assert response.status_code == 200
    assert response.json() == {
        "dispatch": 4,
        "voice": 3,
        "telegram": 2,
        "email": 1,
        "escalation": 5,
        "dlq": 6,
    }


def test_metrics_ops(client, monkeypatch):
    fake_redis = FakeRedis(
        lengths={},
        dlq_entries=[],
        hashes={
            main.prefixed_redis_key(main.METRICS_KEY): {
                b"tasks_sent_by_channel:voice": b"3",
                b"tasks_failed_by_channel:email": b"2",
                b"dlq_size": b"4",
                b"updated_at": b"2026-03-20T10:00:00+00:00",
                b"note": b"hello",
            }
        },
    )
    monkeypatch.setattr(main, "redis_conn", fake_redis)

    response = client.get("/metrics/ops")

    assert response.status_code == 200
    payload = response.json()
    assert payload["redis_key"] == main.prefixed_redis_key(main.METRICS_KEY)
    assert payload["metrics"]["tasks_sent_by_channel:voice"] == 3
    assert payload["metrics"]["tasks_failed_by_channel:email"] == 2
    assert payload["metrics"]["dlq_size"] == 4
    assert payload["metrics"]["updated_at"] == "2026-03-20T10:00:00+00:00"
    assert payload["metrics"]["note"] == "hello"


def test_ops_dlq_preview_and_replay_auth_and_limit(client, monkeypatch):
    monkeypatch.setenv("SERIEMA_ADMIN_TOKEN", "admin-secret")
    fake_redis = FakeRedis(
        lengths={main.prefixed_redis_key(main.DLQ_QUEUE_NAME): 1},
        dlq_entries=[
            b'{"task_name":"email_worker","trace_id":"t1","notification_id":"n1","incident_id":"i1","error":"boom","args":["n1","t1"],"kwargs":{},"failed_at":"stamp"}'
        ],
    )
    monkeypatch.setattr(main, "redis_conn", fake_redis)
    monkeypatch.setattr(main, "replay_dlq", FakeReplayTask({"replayed": 1, "remaining": 0}))
    monkeypatch.setattr(main.celery_app.conf, "task_always_eager", True, raising=False)

    unauthorized = client.get("/ops/dlq/preview?limit=1", headers={"X-Admin-Token": "wrong"})
    assert unauthorized.status_code == 401

    over_limit_preview = client.get("/ops/dlq/preview?limit=101", headers={"X-Admin-Token": "admin-secret"})
    assert over_limit_preview.status_code == 400

    over_limit_replay = client.post("/ops/dlq/replay?limit=101", headers={"X-Admin-Token": "admin-secret"})
    assert over_limit_replay.status_code == 400

    preview = client.get("/ops/dlq/preview?limit=1", headers={"X-Admin-Token": "admin-secret"})
    assert preview.status_code == 200
    payload = preview.json()
    assert payload["total_items"] == 1
    assert payload["items"][0]["task_name"] == "email_worker"
    assert payload["items"][0]["trace_id"] == "t1"

    replay = client.post("/ops/dlq/replay?limit=1", headers={"X-Admin-Token": "admin-secret"})
    assert replay.status_code == 200
    replay_payload = replay.json()
    assert replay_payload["status"] == "completed"
    assert replay_payload["result"]["replayed"] == 1
