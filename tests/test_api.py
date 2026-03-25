import hashlib
import hmac
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.sql.elements import BinaryExpression

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
        return self.lengths.get(
            key, len(self.dlq_entries) if key.endswith(":dlq") else 0
        )

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
            self.dlq_entries.append(
                value.encode("utf-8") if isinstance(value, str) else value
            )
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


@dataclass
class IncidentFixture:
    id: str
    external_event_id: str
    source: str
    severity: str
    title: str
    message: str | None
    payload_json: dict | None
    status: object
    matched_rule_id: str | None
    dedupe_key: str | None
    created_at: datetime
    service: str | None = None
    updated_at: datetime | None = None
    acknowledged_at: datetime | None = None
    acknowledged_by: str | None = None


@dataclass
class NotificationFixture:
    id: str
    incident_id: str
    contact_id: str
    channel: models.NotificationChannel
    status: models.NotificationStatus


class FakeIncidentQuery:
    def __init__(self, incidents, count_mode=False):
        self._incidents = list(incidents)
        self._count_mode = count_mode
        self._filters = []
        self._offset = 0
        self._limit = None
        self._ordered = False

    def filter(self, *criteria):
        self._filters.extend(criteria)
        return self

    def order_by(self, *criteria):
        self._ordered = True
        return self

    def with_for_update(self):
        return self

    def offset(self, value):
        self._offset = value
        return self

    def limit(self, value):
        self._limit = value
        return self

    def _matches(self, incident):
        for criterion in self._filters:
            if isinstance(criterion, BinaryExpression):
                field = getattr(criterion.left, "key", None) or getattr(
                    criterion.left, "name", None
                )
                expected = getattr(criterion.right, "value", None)
                if field is None:
                    continue
                actual = getattr(incident, field)
                if actual != expected and str(actual) != str(expected):
                    return False
        return True

    def _filtered(self):
        items = [incident for incident in self._incidents if self._matches(incident)]
        if self._ordered:
            items.sort(
                key=lambda incident: (incident.created_at, incident.id), reverse=True
            )
        if self._offset:
            items = items[self._offset :]
        if self._limit is not None:
            items = items[: self._limit]
        return items

    def scalar(self):
        if self._count_mode:
            return len(
                [incident for incident in self._incidents if self._matches(incident)]
            )
        return None

    def all(self):
        return self._filtered()

    def first(self):
        items = self._filtered()
        return items[0] if items else None


class FakeIncidentSession(FakeSession):
    def __init__(self, incidents):
        super().__init__(queries=[])
        self._incidents = list(incidents)

    def query(self, *args, **kwargs):
        if len(args) == 1 and args[0] is models.Incident:
            return FakeIncidentQuery(self._incidents, count_mode=False)
        return FakeIncidentQuery(self._incidents, count_mode=True)


class FakeModelQuery:
    def __init__(self, items):
        self._items = list(items)
        self._filters = []
        self._ordered = False
        self._offset = 0
        self._limit = None

    def filter(self, *criteria):
        self._filters.extend(criteria)
        return self

    def order_by(self, *criteria):
        self._ordered = True
        return self

    def with_for_update(self):
        return self

    def offset(self, value):
        self._offset = value
        return self

    def limit(self, value):
        self._limit = value
        return self

    def _matches(self, item):
        for criterion in self._filters:
            if isinstance(criterion, BinaryExpression):
                field = getattr(criterion.left, "key", None) or getattr(
                    criterion.left, "name", None
                )
                expected = getattr(criterion.right, "value", None)
                if field is None:
                    continue
                actual = getattr(item, field, None)
                if actual != expected and str(actual) != str(expected):
                    return False
        return True

    def _filtered(self):
        items = [item for item in self._items if self._matches(item)]
        if self._ordered and items and hasattr(items[0], "created_at"):
            items.sort(
                key=lambda item: (getattr(item, "created_at"), getattr(item, "id", ""))
            )
        if self._offset:
            items = items[self._offset :]
        if self._limit is not None:
            items = items[: self._limit]
        return items

    def count(self):
        return len([item for item in self._items if self._matches(item)])

    def all(self):
        return self._filtered()

    def first(self):
        items = self._filtered()
        return items[0] if items else None


class FakeLifecycleSession(FakeIncidentSession):
    def __init__(self, incidents, audit_logs=None):
        super().__init__(incidents)
        self._audit_logs = list(audit_logs or [])
        self.added_objects = []

    def query(self, *args, **kwargs):
        if len(args) == 1 and args[0] is models.Incident:
            return FakeIncidentQuery(self._incidents, count_mode=False)
        if len(args) == 1 and args[0] is models.AuditLog:
            return FakeModelQuery(self._audit_logs)
        if len(args) == 1 and args[0] is models.Notification:
            return FakeModelQuery(getattr(self, "_notifications", []))
        return FakeIncidentQuery(self._incidents, count_mode=True)

    def add(self, obj, *args, **kwargs):
        self.added_objects.append(obj)
        if isinstance(obj, models.AuditLog):
            self._audit_logs.append(obj)
        return None


class FakeVoiceSession(FakeLifecycleSession):
    def __init__(self, incidents, notifications, audit_logs=None):
        super().__init__(incidents, audit_logs=audit_logs)
        self._notifications = list(notifications)


@dataclass
class RuleFixture:
    id: str
    rule_name: str
    condition_json: dict
    recipient_group_id: str
    channels: list[str]
    active: bool
    priority: int
    requires_ack: bool
    ack_deadline: int | None
    fallback_policy_json: dict | None
    dedupe_window_seconds: int | None = None
    dedupe_fields_json: list[str] | None = None
    notification_templates_json: dict | None = None
    runbook_url: str | None = None
    channel_retry_policy_json: dict | None = None


@dataclass
class GroupFixture:
    id: str
    name: str
    description: str | None = None


class FakeRuleQuery:
    def __init__(self, items):
        self._items = list(items)
        self._filters = []
        self._offset = 0
        self._limit = None
        self._ordered = False

    def filter(self, *criteria):
        self._filters.extend(criteria)
        return self

    def order_by(self, *criteria):
        self._ordered = True
        return self

    def offset(self, value):
        self._offset = value
        return self

    def limit(self, value):
        self._limit = value
        return self

    def _matches(self, item):
        for criterion in self._filters:
            if isinstance(criterion, BinaryExpression):
                field = getattr(criterion.left, "key", None) or getattr(
                    criterion.left, "name", None
                )
                expected = getattr(criterion.right, "value", None)
                if expected is None:
                    right_repr = str(criterion.right).strip().lower()
                    if right_repr == "true":
                        expected = True
                    elif right_repr == "false":
                        expected = False
                if field is None:
                    continue
                actual = getattr(item, field)
                if actual != expected and str(actual) != str(expected):
                    return False
        return True

    def _filtered(self):
        items = [item for item in self._items if self._matches(item)]
        if self._ordered and items and hasattr(items[0], "priority"):
            items.sort(key=lambda item: (item.priority, item.rule_name, item.id))
        if self._offset:
            items = items[self._offset :]
        if self._limit is not None:
            items = items[: self._limit]
        return items

    def all(self):
        return self._filtered()

    def first(self):
        items = self._filtered()
        return items[0] if items else None


class FakeRuleSession(FakeSession):
    def __init__(self, rules, groups):
        super().__init__(queries=[])
        self._rules = list(rules)
        self._groups = list(groups)

    def query(self, *args, **kwargs):
        if len(args) == 1 and args[0] is models.Rule:
            return FakeRuleQuery(self._rules)
        if len(args) == 1 and args[0] is models.Group:
            return FakeRuleQuery(self._groups)
        raise AssertionError(f"Unexpected query call: {args}")


class FakeDirectoryQuery:
    def __init__(self, items):
        self._items = list(items)
        self._filters = []
        self._ordered = False
        self._offset = 0
        self._limit = None

    def filter(self, *criteria):
        self._filters.extend(criteria)
        return self

    def order_by(self, *criteria):
        self._ordered = True
        return self

    def offset(self, value):
        self._offset = value
        return self

    def limit(self, value):
        self._limit = value
        return self

    def _matches(self, item):
        for criterion in self._filters:
            if isinstance(criterion, BinaryExpression):
                field = getattr(criterion.left, "key", None) or getattr(
                    criterion.left, "name", None
                )
                expected = getattr(criterion.right, "value", None)
                if expected is None:
                    right_repr = str(criterion.right).strip().lower()
                    if right_repr == "true":
                        expected = True
                    elif right_repr == "false":
                        expected = False
                if field is None:
                    continue
                actual = getattr(item, field, None)
                if actual != expected and str(actual) != str(expected):
                    return False
        return True

    def _filtered(self):
        items = [item for item in self._items if self._matches(item)]
        if self._ordered and items:
            sample = items[0]
            if hasattr(sample, "name"):
                items.sort(
                    key=lambda item: (
                        getattr(item, "name", ""),
                        str(getattr(item, "id", "")),
                    )
                )
            elif hasattr(sample, "rule_name"):
                items.sort(
                    key=lambda item: (
                        getattr(item, "rule_name", ""),
                        str(getattr(item, "id", "")),
                    )
                )
            elif hasattr(sample, "created_at"):
                items.sort(
                    key=lambda item: (
                        getattr(item, "created_at", None),
                        getattr(item, "id", None),
                    )
                )
            elif hasattr(sample, "group_id") and hasattr(sample, "contact_id"):
                items.sort(
                    key=lambda item: (
                        str(getattr(item, "group_id", "")),
                        str(getattr(item, "contact_id", "")),
                    )
                )
        if self._offset:
            items = items[self._offset :]
        if self._limit is not None:
            items = items[: self._limit]
        return items

    def all(self):
        return self._filtered()

    def first(self):
        items = self._filtered()
        return items[0] if items else None

    def count(self):
        return len([item for item in self._items if self._matches(item)])


class FakeDirectorySession(FakeSession):
    def __init__(
        self,
        contacts=None,
        groups=None,
        group_members=None,
        notifications=None,
        rules=None,
    ):
        super().__init__(queries=[])
        self._contacts = list(contacts or [])
        self._groups = list(groups or [])
        self._group_members = list(group_members or [])
        self._notifications = list(notifications or [])
        self._rules = list(rules or [])
        self.added_objects = []
        self.deleted_objects = []

    def query(self, *args, **kwargs):
        if len(args) != 1:
            raise AssertionError(f"Unexpected query call: {args}")
        model = args[0]
        if model is models.Contact:
            return FakeDirectoryQuery(self._contacts)
        if model is models.Group:
            return FakeDirectoryQuery(self._groups)
        if model is models.GroupMember:
            return FakeDirectoryQuery(self._group_members)
        if model is models.Notification:
            return FakeDirectoryQuery(self._notifications)
        if model is models.Rule:
            return FakeDirectoryQuery(self._rules)
        raise AssertionError(f"Unexpected query call: {args}")

    def add(self, obj, *args, **kwargs):
        self.added_objects.append(obj)
        if hasattr(obj, "id") and getattr(obj, "id", None) is None:
            obj.id = uuid.uuid4()
        if isinstance(obj, models.Contact):
            self._contacts.append(obj)
        elif isinstance(obj, models.Group):
            self._groups.append(obj)
        elif isinstance(obj, models.GroupMember):
            self._group_members.append(obj)
        elif isinstance(obj, models.Notification):
            self._notifications.append(obj)
        elif isinstance(obj, models.Rule):
            self._rules.append(obj)
        return None

    def delete(self, obj, *args, **kwargs):
        self.deleted_objects.append(obj)
        collections = [
            self._contacts,
            self._groups,
            self._group_members,
            self._notifications,
            self._rules,
        ]
        for collection in collections:
            for index, item in enumerate(list(collection)):
                if self._same_record(item, obj):
                    collection.pop(index)
                    return None
        return None

    def _same_record(self, item, target):
        compared = False
        for attr in ("id", "group_id", "contact_id", "incident_id"):
            if hasattr(item, attr) and hasattr(target, attr):
                compared = True
                if getattr(item, attr) != getattr(target, attr):
                    return False
        return compared and True


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


def test_oasis_radar_service_mapping():
    assert main._oasis_radar_service({"compose_service": "payments"}) == "payments"
    assert main._oasis_radar_service({"service": "api"}) == "api"
    assert main._oasis_radar_service({}) == "oasis-radar"


def test_oasis_radar_severity_mapping():
    assert main._oasis_radar_severity('{"level":"error"}', {}) == "ERROR"
    assert main._oasis_radar_severity("panic in worker", {}) == "CRITICAL"
    assert main._oasis_radar_severity("warn threshold reached", {}) == "WARN"
    assert main._oasis_radar_severity("normal heartbeat", {}) == "INFO"


def test_oasis_radar_pull_error_updates_metrics(client, monkeypatch):
    class RadarRedis:
        def __init__(self):
            self.hash = {}

        def hincrby(self, key, field, value):
            current = int(self.hash.get(field, 0))
            self.hash[field] = current + int(value)
            return self.hash[field]

        def hset(self, key, field, value):
            self.hash[field] = value
            return 1

    fake_redis = RadarRedis()
    fake_session = FakeSession(queries=[])
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    monkeypatch.setattr(main, "redis_conn", fake_redis)
    monkeypatch.setenv("SERIEMA_ADMIN_TOKEN", "test-admin-token")

    from urllib.error import URLError

    monkeypatch.setattr(
        main,
        "_fetch_oasis_radar_entries",
        lambda query, lookback_seconds, limit: (_ for _ in ()).throw(
            URLError("network down")
        ),
    )
    try:
        response = client.post(
            "/integrations/oasis-radar/pull",
            headers={"X-Admin-Token": "test-admin-token"},
        )
        assert response.status_code == 502
        assert fake_redis.hash["oasis_radar_pull_failed_total"] == 1
        assert fake_redis.hash["oasis_radar_pull_last_status"] == "connectivity_error"
    finally:
        main.app.dependency_overrides.pop(main.get_db, None)


def test_ingest_event_duplicate_records_duplicate_audit(client, monkeypatch):
    fake_session = FakeLifecycleSession([], audit_logs=[])
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    monkeypatch.setattr(main, "is_duplicate", lambda key: True)
    try:
        response = client.post(
            "/events/incoming",
            json={
                "source": "prometheus",
                "external_event_id": "evt-dup-1",
                "severity": "CRITICAL",
                "title": "Duplicate event",
                "message": "dup",
                "payload_json": {"a": 1},
                "dedupe_key": "dup-key-1",
            },
        )

        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] == "ignored"
        assert payload["reason"] == "duplicate"
        assert payload["incident_id"] is None
        assert payload["matched_rule"] is False
        assert any(
            isinstance(obj, models.AuditLog)
            and obj.action == models.AuditAction.DUPLICATED_EVENT
            for obj in fake_session.added_objects
        )
        duplicate_audit = next(
            obj
            for obj in fake_session.added_objects
            if isinstance(obj, models.AuditLog)
            and obj.action == models.AuditAction.DUPLICATED_EVENT
        )
        assert duplicate_audit.incident_id is None
        assert duplicate_audit.details_json["dedupe_key"] == "dup-key-1"
    finally:
        main.app.dependency_overrides.pop(main.get_db, None)


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


def test_alerts_ops_without_alerts(client, monkeypatch):
    fake_session = FakeSession(queries=[])
    fake_redis = FakeRedis(
        lengths={
            main.queue_name("dispatch"): 2,
            main.queue_name("voice"): 1,
            main.queue_name("telegram"): 0,
            main.queue_name("email"): 0,
            main.queue_name("escalation"): 0,
            main.prefixed_redis_key(main.DLQ_QUEUE_NAME): 0,
        },
        dlq_entries=[],
        hashes={
            main.prefixed_redis_key(main.METRICS_KEY): {
                b"dlq_size": b"0",
                b"updated_at": b"2026-03-20T10:00:00+00:00",
            }
        },
    )
    monkeypatch.setattr(main, "redis_conn", fake_redis)
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    monkeypatch.setattr(
        main,
        "_calculate_sla_metrics",
        lambda db, hours=24: schemas.SLAMetricsResponse(
            hours=24,
            window_start=datetime(2026, 3, 20, 9, 0, 0),
            window_end=datetime(2026, 3, 20, 10, 0, 0),
            total_incidents=10,
            acknowledged_incidents=9,
            acknowledgement_rate=0.9,
            average_tta_seconds=12.0,
            incidents_by_status=[
                schemas.IncidentStatusCount(status="OPEN", count=1),
                schemas.IncidentStatusCount(status="ACKNOWLEDGED", count=9),
            ],
        ),
    )

    response = client.get("/alerts/ops")

    assert response.status_code == 200
    payload = response.json()
    assert payload["overall_severity"] == "ok"
    assert payload["alert_count"] == 0
    assert payload["alerts"] == []
    assert payload["metrics"]["dlq_size"] == 0
    assert payload["queue_metrics"]["dispatch"] == 2


def test_alerts_ops_with_alerts(client, monkeypatch):
    fake_session = FakeSession(queries=[])
    fake_redis = FakeRedis(
        lengths={
            main.queue_name("dispatch"): 50,
            main.queue_name("voice"): 30,
            main.queue_name("telegram"): 0,
            main.queue_name("email"): 0,
            main.queue_name("escalation"): 0,
            main.prefixed_redis_key(main.DLQ_QUEUE_NAME): 60,
        },
        dlq_entries=[],
        hashes={
            main.prefixed_redis_key(main.METRICS_KEY): {
                b"dlq_size": b"60",
                b"updated_at": b"2026-03-20T10:00:00+00:00",
            }
        },
    )
    monkeypatch.setattr(main, "redis_conn", fake_redis)
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    monkeypatch.setattr(main, "ALERT_DLQ_WARN", 10)
    monkeypatch.setattr(main, "ALERT_DLQ_CRIT", 50)
    monkeypatch.setattr(main, "ALERT_QUEUE_WARN", 20)
    monkeypatch.setattr(main, "ALERT_ACK_RATE_WARN", 0.8)
    monkeypatch.setattr(
        main,
        "_calculate_sla_metrics",
        lambda db, hours=24: schemas.SLAMetricsResponse(
            hours=24,
            window_start=datetime(2026, 3, 20, 9, 0, 0),
            window_end=datetime(2026, 3, 20, 10, 0, 0),
            total_incidents=10,
            acknowledged_incidents=3,
            acknowledgement_rate=0.3,
            average_tta_seconds=120.0,
            incidents_by_status=[
                schemas.IncidentStatusCount(status="OPEN", count=7),
                schemas.IncidentStatusCount(status="ACKNOWLEDGED", count=3),
            ],
        ),
    )

    response = client.get("/alerts/ops")

    assert response.status_code == 200
    payload = response.json()
    assert payload["overall_severity"] == "critical"
    assert payload["alert_count"] == 4
    alert_types = {alert["alert_type"] for alert in payload["alerts"]}
    assert alert_types == {
        "dlq_size",
        "queue_backlog:dispatch",
        "queue_backlog:voice",
        "ack_rate_24h",
    }
    severities = {alert["severity"] for alert in payload["alerts"]}
    assert "critical" in severities
    assert "warn" in severities
    assert payload["metrics"]["dlq_size"] == 60
    assert payload["queue_metrics"]["voice"] == 30


def test_ops_dlq_preview_and_replay_auth_and_limit(client, monkeypatch):
    monkeypatch.setenv("SERIEMA_ADMIN_TOKEN", "admin-secret")
    fake_redis = FakeRedis(
        lengths={main.prefixed_redis_key(main.DLQ_QUEUE_NAME): 1},
        dlq_entries=[
            b'{"task_name":"email_worker","trace_id":"t1","notification_id":"n1","incident_id":"i1","error":"boom","args":["n1","t1"],"kwargs":{},"failed_at":"stamp"}'
        ],
    )
    monkeypatch.setattr(main, "redis_conn", fake_redis)
    monkeypatch.setattr(
        main, "replay_dlq", FakeReplayTask({"replayed": 1, "remaining": 0})
    )
    monkeypatch.setattr(main.celery_app.conf, "task_always_eager", True, raising=False)

    unauthorized = client.get(
        "/ops/dlq/preview?limit=1", headers={"X-Admin-Token": "wrong"}
    )
    assert unauthorized.status_code == 401

    over_limit_preview = client.get(
        "/ops/dlq/preview?limit=101", headers={"X-Admin-Token": "admin-secret"}
    )
    assert over_limit_preview.status_code == 400

    over_limit_replay = client.post(
        "/ops/dlq/replay?limit=101", headers={"X-Admin-Token": "admin-secret"}
    )
    assert over_limit_replay.status_code == 400

    preview = client.get(
        "/ops/dlq/preview?limit=1", headers={"X-Admin-Token": "admin-secret"}
    )
    assert preview.status_code == 200
    payload = preview.json()
    assert payload["total_items"] == 1
    assert payload["items"][0]["task_name"] == "email_worker"
    assert payload["items"][0]["trace_id"] == "t1"

    replay = client.post(
        "/ops/dlq/replay?limit=1", headers={"X-Admin-Token": "admin-secret"}
    )
    assert replay.status_code == 200
    replay_payload = replay.json()
    assert replay_payload["status"] == "completed"
    assert replay_payload["result"]["replayed"] == 1


def test_ops_dlq_replay_last_without_report(client, monkeypatch):
    monkeypatch.setattr(main, "get_dlq_replay_report", lambda: {})

    response = client.get("/ops/dlq/replay/last")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "empty"
    assert payload["started_at"] is None
    assert payload["finished_at"] is None
    assert payload["requested_limit"] is None
    assert payload["effective_limit"] is None
    assert payload["replayed"] == 0
    assert payload["remaining"] == 0
    assert payload["dry_run"] is False
    assert payload["locked"] is False


def test_ops_dlq_replay_last_with_report(client, monkeypatch):
    monkeypatch.setattr(
        main,
        "get_dlq_replay_report",
        lambda: {
            "status": "completed",
            "started_at": "2026-03-20T10:00:00+00:00",
            "finished_at": "2026-03-20T10:01:00+00:00",
            "requested_limit": 20,
            "effective_limit": 5,
            "replayed": 4,
            "remaining": 1,
            "dry_run": 0,
            "locked": 1,
        },
    )

    response = client.get("/ops/dlq/replay/last")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "completed"
    assert payload["started_at"] == "2026-03-20T10:00:00+00:00"
    assert payload["finished_at"] == "2026-03-20T10:01:00+00:00"
    assert payload["requested_limit"] == 20
    assert payload["effective_limit"] == 5
    assert payload["replayed"] == 4
    assert payload["remaining"] == 1
    assert payload["dry_run"] is False
    assert payload["locked"] is True


def test_ops_integration_status_empty(client, monkeypatch):
    fake_session = FakeLifecycleSession([], audit_logs=[])
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    monkeypatch.setattr(main, "get_dlq_replay_report", lambda: {})
    try:
        response = client.get("/ops/integration/status")

        assert response.status_code == 200
        payload = response.json()
        assert payload["enums_ok"] is True
        assert payload["fallback_contract_ok"] is True
        assert payload["trace_propagation_signal"] is False
        assert payload["duplicate_event_signal"] is False
        assert payload["ack_flow_signal"] is False
        assert payload["escalation_guard_signal"] is False
        assert payload["dlq_reporting_signal"] is False
        assert payload["evidence"]["trace_propagation"]["trace_logs"] == 0
        assert payload["evidence"]["duplicate_event"]["duplicate_event_logs"] == 0
        assert payload["evidence"]["ack_flow"]["ack_received_logs"] == 0
        assert payload["evidence"]["dlq_reporting"]["report_status"] is None
    finally:
        main.app.dependency_overrides.pop(main.get_db, None)


def test_ops_integration_status_with_minimal_signals(client, monkeypatch):
    incident_ack = IncidentFixture(
        id="00000000-0000-0000-0000-00000000e101",
        external_event_id="evt-int-1",
        source="prometheus",
        severity="CRITICAL",
        title="Ack incident",
        message="m",
        payload_json={"k": "v"},
        status=models.IncidentStatus.ACKNOWLEDGED,
        matched_rule_id=None,
        dedupe_key=None,
        created_at=datetime(2026, 3, 20, 15, 0, 0),
        acknowledged_at=datetime(2026, 3, 20, 15, 1, 0),
        acknowledged_by="alice",
    )
    incident_open = IncidentFixture(
        id="00000000-0000-0000-0000-00000000e102",
        external_event_id="evt-int-2",
        source="grafana",
        severity="WARN",
        title="Escalation incident",
        message="m",
        payload_json={"k": "v"},
        status=models.IncidentStatus.OPEN,
        matched_rule_id=None,
        dedupe_key=None,
        created_at=datetime(2026, 3, 20, 16, 0, 0),
    )
    audit_logs = [
        models.AuditLog(
            id=uuid.UUID("00000000-0000-0000-0000-00000000e201"),
            trace_id="trace-int-1",
            incident_id=uuid.UUID(incident_ack.id),
            action=models.AuditAction.EVENT_RECEIVED,
            details_json={"kind": "event"},
            created_at=datetime(2026, 3, 20, 15, 0, 30),
        ),
        models.AuditLog(
            id=uuid.UUID("00000000-0000-0000-0000-00000000e202"),
            trace_id="trace-int-2",
            incident_id=uuid.UUID(incident_ack.id),
            action=models.AuditAction.ACK_RECEIVED,
            details_json={"channel": "api"},
            created_at=datetime(2026, 3, 20, 15, 1, 0),
        ),
        models.AuditLog(
            id=uuid.UUID("00000000-0000-0000-0000-00000000e203"),
            trace_id="trace-int-3",
            incident_id=uuid.UUID(incident_open.id),
            action=models.AuditAction.ESCALATED,
            details_json={"reason": "timeout"},
            created_at=datetime(2026, 3, 20, 16, 5, 0),
        ),
        models.AuditLog(
            id=uuid.UUID("00000000-0000-0000-0000-00000000e204"),
            trace_id="trace-int-4",
            incident_id=None,
            action=models.AuditAction.DUPLICATED_EVENT,
            details_json={"dedupe_key": "dup-1"},
            created_at=datetime(2026, 3, 20, 16, 10, 0),
        ),
    ]
    fake_session = FakeLifecycleSession(
        [incident_ack, incident_open], audit_logs=audit_logs
    )
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    monkeypatch.setattr(
        main, "get_dlq_replay_report", lambda: {"status": "completed", "replayed": 1}
    )
    try:
        response = client.get("/ops/integration/status")

        assert response.status_code == 200
        payload = response.json()
        assert payload["enums_ok"] is True
        assert payload["fallback_contract_ok"] is True
        assert payload["trace_propagation_signal"] is True
        assert payload["duplicate_event_signal"] is True
        assert payload["ack_flow_signal"] is True
        assert payload["escalation_guard_signal"] is True
        assert payload["dlq_reporting_signal"] is True
        assert payload["evidence"]["trace_propagation"]["trace_logs"] == 4
        assert payload["evidence"]["duplicate_event"]["duplicate_event_logs"] == 1
        assert payload["evidence"]["ack_flow"]["ack_received_logs"] == 1
        assert payload["evidence"]["ack_flow"]["acknowledged_incidents"] == 1
        assert payload["evidence"]["escalation_guard"]["escalated_logs"] == 1
        assert payload["evidence"]["escalation_guard"]["non_open_incidents"] == 1
        assert (
            payload["evidence"]["escalation_guard"]["non_open_escalated_incidents"] == 0
        )
        assert payload["evidence"]["dlq_reporting"]["report_status"] == "completed"
    finally:
        main.app.dependency_overrides.pop(main.get_db, None)


def test_ops_readiness_empty_degraded(client, monkeypatch):
    fake_session = FakeLifecycleSession([], audit_logs=[])
    fake_redis = FakeRedis(lengths={}, dlq_entries=[], hashes={})
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    monkeypatch.setattr(main, "redis_conn", fake_redis)
    monkeypatch.setattr(main, "get_dlq_replay_report", lambda: {})
    try:
        response = client.get("/ops/readiness")

        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] == "red"
        assert payload["score"] < 70
        assert any(not check["passed"] for check in payload["checks"])
        assert payload["blockers"]
        assert "ack_flow_signal" in payload["blockers"]
        assert (
            payload["evidence"]["heartbeats"]["queue_metrics_snapshot"]["last_run_at"]
            is None
        )
        assert (
            payload["evidence"]["heartbeats"]["queue_metrics_snapshot"]["last_status"]
            is None
        )
    finally:
        main.app.dependency_overrides.pop(main.get_db, None)


def test_ops_readiness_healthy(client, monkeypatch):
    incident_ack = IncidentFixture(
        id="00000000-0000-0000-0000-00000000f101",
        external_event_id="evt-ready-1",
        source="prometheus",
        severity="CRITICAL",
        title="Ready ack",
        message="m",
        payload_json={"k": "v"},
        status=models.IncidentStatus.ACKNOWLEDGED,
        matched_rule_id=None,
        dedupe_key=None,
        created_at=datetime(2026, 3, 20, 17, 0, 0),
        acknowledged_at=datetime(2026, 3, 20, 17, 1, 0),
        acknowledged_by="alice",
    )
    incident_open = IncidentFixture(
        id="00000000-0000-0000-0000-00000000f102",
        external_event_id="evt-ready-2",
        source="grafana",
        severity="WARN",
        title="Ready escalate",
        message="m",
        payload_json={"k": "v"},
        status=models.IncidentStatus.OPEN,
        matched_rule_id=None,
        dedupe_key=None,
        created_at=datetime(2026, 3, 20, 18, 0, 0),
    )
    audit_logs = [
        models.AuditLog(
            id=uuid.UUID("00000000-0000-0000-0000-00000000f201"),
            trace_id="trace-ready-1",
            incident_id=uuid.UUID(incident_ack.id),
            action=models.AuditAction.EVENT_RECEIVED,
            details_json={"kind": "event"},
            created_at=datetime(2026, 3, 20, 17, 0, 30),
        ),
        models.AuditLog(
            id=uuid.UUID("00000000-0000-0000-0000-00000000f202"),
            trace_id="trace-ready-2",
            incident_id=uuid.UUID(incident_ack.id),
            action=models.AuditAction.ACK_RECEIVED,
            details_json={"channel": "api"},
            created_at=datetime(2026, 3, 20, 17, 1, 0),
        ),
        models.AuditLog(
            id=uuid.UUID("00000000-0000-0000-0000-00000000f203"),
            trace_id="trace-ready-3",
            incident_id=uuid.UUID(incident_open.id),
            action=models.AuditAction.ESCALATED,
            details_json={"reason": "timeout"},
            created_at=datetime(2026, 3, 20, 18, 5, 0),
        ),
        models.AuditLog(
            id=uuid.UUID("00000000-0000-0000-0000-00000000f204"),
            trace_id="trace-ready-4",
            incident_id=None,
            action=models.AuditAction.DUPLICATED_EVENT,
            details_json={"dedupe_key": "dup-ready"},
            created_at=datetime(2026, 3, 20, 18, 10, 0),
        ),
    ]
    fake_session = FakeLifecycleSession(
        [incident_ack, incident_open], audit_logs=audit_logs
    )
    fake_redis = FakeRedis(
        lengths={},
        dlq_entries=[],
        hashes={
            main.prefixed_redis_key(main.METRICS_KEY): {
                b"last_run_at:queue_metrics_snapshot": b"2026-03-20T18:15:00+00:00",
                b"last_status:queue_metrics_snapshot": b"ok",
                b"last_run_at:prune_dlq": b"2026-03-20T18:16:00+00:00",
                b"last_status:prune_dlq": b"ok",
                b"last_run_at:stale_incident_sweeper": b"2026-03-20T18:17:00+00:00",
                b"last_status:stale_incident_sweeper": b"ok",
            }
        },
    )
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    monkeypatch.setattr(main, "redis_conn", fake_redis)
    monkeypatch.setattr(
        main, "get_dlq_replay_report", lambda: {"status": "completed", "replayed": 1}
    )
    try:
        response = client.get("/ops/readiness")

        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] == "green"
        assert payload["score"] == 100
        assert payload["blockers"] == []
        assert all(check["passed"] is True for check in payload["checks"])
        assert (
            payload["evidence"]["heartbeats"]["queue_metrics_snapshot"]["last_status"]
            == "ok"
        )
        assert (
            payload["evidence"]["integration"]["dlq_reporting"]["report_status"]
            == "completed"
        )
    finally:
        main.app.dependency_overrides.pop(main.get_db, None)


def test_contacts_list_get_patch_delete_happy(client):
    contact_a = models.Contact(
        id=uuid.UUID("00000000-0000-0000-0000-00000000aa01"),
        name="Alpha",
        email="alpha@example.com",
        phone=None,
        whatsapp=None,
        telegram_id=None,
    )
    contact_b = models.Contact(
        id=uuid.UUID("00000000-0000-0000-0000-00000000aa02"),
        name="Beta",
        email="beta@example.com",
        phone=None,
        whatsapp=None,
        telegram_id=None,
    )
    fake_session = FakeDirectorySession(contacts=[contact_b, contact_a])
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    try:
        response = client.get("/contacts?limit=1&offset=1")
        assert response.status_code == 200
        payload = response.json()
        assert payload["total"] == 2
        assert payload["limit"] == 1
        assert payload["offset"] == 1
        assert payload["items"][0]["id"] == str(contact_b.id)

        detail = client.get(f"/contacts/{contact_a.id}")
        assert detail.status_code == 200
        assert detail.json()["name"] == "Alpha"

        limit_invalid = client.get(f"/contacts?limit={main.OPS_ENDPOINT_MAX_LIMIT + 1}")
        assert limit_invalid.status_code == 400

        updated = client.patch(f"/contacts/{contact_a.id}", json={"phone": "555-0101"})
        assert updated.status_code == 200
        assert updated.json()["phone"] == "555-0101"

        deleted = client.delete(f"/contacts/{contact_b.id}")
        assert deleted.status_code == 204

        missing = client.get(f"/contacts/{contact_b.id}")
        assert missing.status_code == 404
    finally:
        main.app.dependency_overrides.pop(main.get_db, None)


def test_contacts_delete_conflicts_and_validation(client):
    contact = models.Contact(
        id=uuid.UUID("00000000-0000-0000-0000-00000000ab01"),
        name="Gamma",
        email="gamma@example.com",
        phone=None,
        whatsapp=None,
        telegram_id=None,
    )
    notification = models.Notification(
        id=uuid.UUID("00000000-0000-0000-0000-00000000ab11"),
        incident_id=uuid.UUID("00000000-0000-0000-0000-00000000ab21"),
        contact_id=contact.id,
        channel=models.NotificationChannel.VOICE,
        status=models.NotificationStatus.PENDING,
    )
    group = models.Group(
        id=uuid.UUID("00000000-0000-0000-0000-00000000ab31"),
        name="group-ab31",
        description=None,
    )
    membership = models.GroupMember(group_id=group.id, contact_id=contact.id)
    fake_session = FakeDirectorySession(
        contacts=[contact],
        notifications=[notification],
        group_members=[membership],
        groups=[group],
    )
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    try:
        conflict = client.delete(f"/contacts/{contact.id}")
        assert conflict.status_code == 409
        assert "notifications" in conflict.json()["detail"].lower()

        invalid = client.delete("/contacts/not-a-uuid")
        assert invalid.status_code == 400

        missing = client.delete("/contacts/00000000-0000-0000-0000-00000000ab02")
        assert missing.status_code == 404
    finally:
        main.app.dependency_overrides.pop(main.get_db, None)


def test_groups_list_member_delete_and_patch_happy(client):
    group_a = models.Group(
        id=uuid.UUID("00000000-0000-0000-0000-00000000ba01"),
        name="Alpha Group",
        description="old",
    )
    group_b = models.Group(
        id=uuid.UUID("00000000-0000-0000-0000-00000000ba02"),
        name="Beta Group",
        description=None,
    )
    contact = models.Contact(
        id=uuid.UUID("00000000-0000-0000-0000-00000000ba11"),
        name="Member",
        email="member@example.com",
        phone=None,
        whatsapp=None,
        telegram_id=None,
    )
    membership = models.GroupMember(group_id=group_a.id, contact_id=contact.id)
    fake_session = FakeDirectorySession(
        groups=[group_b, group_a], contacts=[contact], group_members=[membership]
    )
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    try:
        response = client.get("/groups?limit=1&offset=0")
        assert response.status_code == 200
        payload = response.json()
        assert payload["total"] == 2
        assert payload["items"][0]["name"] == "Alpha Group"

        detail = client.get(f"/groups/{group_a.id}")
        assert detail.status_code == 200
        assert detail.json()["description"] == "old"

        updated = client.patch(f"/groups/{group_a.id}", json={"description": "new"})
        assert updated.status_code == 200
        assert updated.json()["description"] == "new"

        member_deleted = client.delete(f"/groups/{group_a.id}/members/{contact.id}")
        assert member_deleted.status_code == 204

        group_deleted = client.delete(f"/groups/{group_a.id}")
        assert group_deleted.status_code == 204

        missing = client.get(f"/groups/{group_a.id}")
        assert missing.status_code == 404
    finally:
        main.app.dependency_overrides.pop(main.get_db, None)


def test_groups_delete_conflicts_and_member_validation(client):
    group = models.Group(
        id=uuid.UUID("00000000-0000-0000-0000-00000000bb01"),
        name="Conflict Group",
        description=None,
    )
    rule = models.Rule(
        id=uuid.UUID("00000000-0000-0000-0000-00000000bb11"),
        rule_name="rule",
        condition_json={"source": "x"},
        recipient_group_id=group.id,
        channels=["voice"],
        active=True,
        priority=10,
        requires_ack=False,
        ack_deadline=None,
        fallback_policy_json=None,
    )
    contact = models.Contact(
        id=uuid.UUID("00000000-0000-0000-0000-00000000bb21"),
        name="Orphan",
        email="orphan@example.com",
        phone=None,
        whatsapp=None,
        telegram_id=None,
    )
    fake_session = FakeDirectorySession(
        groups=[group], rules=[rule], contacts=[contact], group_members=[]
    )
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    try:
        conflict = client.delete(f"/groups/{group.id}")
        assert conflict.status_code == 409
        assert "rules" in conflict.json()["detail"].lower()

        invalid = client.delete("/groups/not-a-uuid")
        assert invalid.status_code == 400

        missing = client.delete("/groups/00000000-0000-0000-0000-00000000bb02")
        assert missing.status_code == 404

        member_invalid = client.delete(f"/groups/{group.id}/members/not-a-uuid")
        assert member_invalid.status_code == 400

        member_missing = client.delete(f"/groups/{group.id}/members/{contact.id}")
        assert member_missing.status_code == 404
    finally:
        main.app.dependency_overrides.pop(main.get_db, None)


def test_list_incidents_filters_and_pagination(client):
    incidents = [
        IncidentFixture(
            id="00000000-0000-0000-0000-000000000001",
            external_event_id="evt-1",
            source="prometheus",
            severity="CRITICAL",
            title="Critical 1",
            message="m1",
            payload_json={"n": 1},
            status=models.IncidentStatus.ACKNOWLEDGED,
            matched_rule_id=None,
            dedupe_key="d1",
            created_at=datetime(2026, 3, 20, 10, 0, 0),
            acknowledged_at=datetime(2026, 3, 20, 10, 10, 0),
            acknowledged_by="1",
        ),
        IncidentFixture(
            id="00000000-0000-0000-0000-000000000002",
            external_event_id="evt-2",
            source="prometheus",
            severity="CRITICAL",
            title="Critical 2",
            message="m2",
            payload_json={"n": 2},
            status=models.IncidentStatus.OPEN,
            matched_rule_id=None,
            dedupe_key="d2",
            created_at=datetime(2026, 3, 20, 11, 0, 0),
        ),
        IncidentFixture(
            id="00000000-0000-0000-0000-000000000003",
            external_event_id="evt-3",
            source="grafana",
            severity="WARN",
            title="Warn 3",
            message="m3",
            payload_json={"n": 3},
            status=models.IncidentStatus.ACKNOWLEDGED,
            matched_rule_id=None,
            dedupe_key="d3",
            created_at=datetime(2026, 3, 20, 12, 0, 0),
            acknowledged_at=datetime(2026, 3, 20, 12, 5, 0),
            acknowledged_by="2",
        ),
    ]
    fake_session = FakeIncidentSession(incidents)
    main.app.dependency_overrides[main.get_db] = lambda: fake_session

    response = client.get(
        "/incidents?status=ACKNOWLEDGED&source=prometheus&severity=CRITICAL&limit=10&offset=0"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["limit"] == 10
    assert payload["offset"] == 0
    assert len(payload["items"]) == 1
    assert payload["items"][0]["id"] == "00000000-0000-0000-0000-000000000001"
    assert payload["items"][0]["source"] == "prometheus"


def test_list_incidents_pagination_and_order(client):
    incidents = [
        IncidentFixture(
            id="00000000-0000-0000-0000-000000000011",
            external_event_id="evt-11",
            source="s1",
            severity="CRITICAL",
            title="11",
            message=None,
            payload_json=None,
            status=models.IncidentStatus.OPEN,
            matched_rule_id=None,
            dedupe_key=None,
            created_at=datetime(2026, 3, 20, 9, 0, 0),
        ),
        IncidentFixture(
            id="00000000-0000-0000-0000-000000000012",
            external_event_id="evt-12",
            source="s2",
            severity="CRITICAL",
            title="12",
            message=None,
            payload_json=None,
            status=models.IncidentStatus.OPEN,
            matched_rule_id=None,
            dedupe_key=None,
            created_at=datetime(2026, 3, 20, 10, 0, 0),
        ),
        IncidentFixture(
            id="00000000-0000-0000-0000-000000000013",
            external_event_id="evt-13",
            source="s3",
            severity="CRITICAL",
            title="13",
            message=None,
            payload_json=None,
            status=models.IncidentStatus.OPEN,
            matched_rule_id=None,
            dedupe_key=None,
            created_at=datetime(2026, 3, 20, 11, 0, 0),
        ),
    ]
    fake_session = FakeIncidentSession(incidents)
    main.app.dependency_overrides[main.get_db] = lambda: fake_session

    response = client.get("/incidents?limit=2&offset=1")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 3
    assert payload["limit"] == 2
    assert payload["offset"] == 1
    assert [item["id"] for item in payload["items"]] == [
        "00000000-0000-0000-0000-000000000012",
        "00000000-0000-0000-0000-000000000011",
    ]


def test_list_incidents_limit_above_max(client):
    response = client.get(f"/incidents?limit={main.OPS_ENDPOINT_MAX_LIMIT + 1}")
    assert response.status_code == 422


def test_ack_incident_success_404_and_409(client):
    incident = IncidentFixture(
        id="00000000-0000-0000-0000-00000000a101",
        external_event_id="evt-ack-1",
        source="pagerduty",
        severity="CRITICAL",
        title="Ack me",
        message="m",
        payload_json={"k": "v"},
        status=models.IncidentStatus.OPEN,
        matched_rule_id=None,
        dedupe_key=None,
        created_at=datetime(2026, 3, 20, 10, 0, 0),
    )
    acknowledged = IncidentFixture(
        id="00000000-0000-0000-0000-00000000a102",
        external_event_id="evt-ack-2",
        source="pagerduty",
        severity="CRITICAL",
        title="Already acked",
        message="m",
        payload_json={"k": "v"},
        status=models.IncidentStatus.ACKNOWLEDGED,
        matched_rule_id=None,
        dedupe_key=None,
        created_at=datetime(2026, 3, 20, 11, 0, 0),
        acknowledged_at=datetime(2026, 3, 20, 11, 5, 0),
        acknowledged_by="existing",
    )
    fake_session = FakeLifecycleSession([incident, acknowledged])
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    try:
        response = client.post(
            f"/incidents/{incident.id}/ack",
            json={"acknowledged_by": "alice"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["action"] == "ack"
        assert payload["incident"]["id"] == incident.id
        assert payload["incident"]["status"] == "ACKNOWLEDGED"
        assert payload["incident"]["acknowledged_by"] == "alice"
        assert payload["incident"]["acknowledged_at"] is not None
        assert any(
            isinstance(obj, models.AuditLog)
            and obj.action == models.AuditAction.ACK_RECEIVED
            for obj in fake_session.added_objects
        )

        missing = client.post(
            "/incidents/00000000-0000-0000-0000-00000000ffff/ack", json={}
        )
        assert missing.status_code == 404

        conflict = client.post(
            f"/incidents/{acknowledged.id}/ack", json={"acknowledged_by": "bob"}
        )
        assert conflict.status_code == 409
    finally:
        main.app.dependency_overrides.pop(main.get_db, None)


def test_resolve_incident_success_404_and_409(client):
    incident = IncidentFixture(
        id="00000000-0000-0000-0000-00000000b101",
        external_event_id="evt-res-1",
        source="grafana",
        severity="WARN",
        title="Resolve me",
        message="m",
        payload_json={"k": "v"},
        status=models.IncidentStatus.ACKNOWLEDGED,
        matched_rule_id=None,
        dedupe_key=None,
        created_at=datetime(2026, 3, 20, 12, 0, 0),
        acknowledged_at=datetime(2026, 3, 20, 12, 2, 0),
        acknowledged_by="alice",
    )
    open_incident = IncidentFixture(
        id="00000000-0000-0000-0000-00000000b102",
        external_event_id="evt-res-2",
        source="grafana",
        severity="WARN",
        title="Still open",
        message="m",
        payload_json={"k": "v"},
        status=models.IncidentStatus.OPEN,
        matched_rule_id=None,
        dedupe_key=None,
        created_at=datetime(2026, 3, 20, 13, 0, 0),
    )
    fake_session = FakeLifecycleSession([incident, open_incident])
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    try:
        response = client.post(
            f"/incidents/{incident.id}/resolve",
            json={"resolved_by": "bob", "note": "fixed"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["action"] == "resolve"
        assert payload["incident"]["id"] == incident.id
        assert payload["incident"]["status"] == "RESOLVED"
        assert any(
            isinstance(obj, models.AuditLog)
            and obj.action == models.AuditAction.CALLBACK_RECEIVED
            for obj in fake_session.added_objects
        )
        assert any(
            isinstance(obj, models.AuditLog)
            and obj.details_json.get("resolved_by") == "bob"
            and obj.details_json.get("note") == "fixed"
            for obj in fake_session.added_objects
        )

        missing = client.post(
            "/incidents/00000000-0000-0000-0000-00000000ffff/resolve", json={}
        )
        assert missing.status_code == 404

        conflict = client.post(
            f"/incidents/{open_incident.id}/resolve", json={"resolved_by": "bob"}
        )
        assert conflict.status_code == 409
    finally:
        main.app.dependency_overrides.pop(main.get_db, None)


def test_voice_callback_ack_is_idempotent(client):
    incident = IncidentFixture(
        id="00000000-0000-0000-0000-00000000c101",
        external_event_id="evt-voice-1",
        source="pagerduty",
        severity="CRITICAL",
        title="Voice ack",
        message="m",
        payload_json={"k": "v"},
        status=models.IncidentStatus.OPEN,
        matched_rule_id=None,
        dedupe_key=None,
        created_at=datetime(2026, 3, 20, 14, 0, 0),
    )
    notification = NotificationFixture(
        id="00000000-0000-0000-0000-00000000c201",
        incident_id=incident.id,
        contact_id="00000000-0000-0000-0000-00000000c301",
        channel=models.NotificationChannel.VOICE,
        status=models.NotificationStatus.SENT,
    )
    fake_session = FakeVoiceSession([incident], [notification])
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    try:
        body = b"Digits=1"
        timestamp = str(int(time.time()))
        signature = hmac.new(
            os.environ.get("VOICE_WEBHOOK_SECRET", "").encode("utf-8"),
            f"{timestamp}.".encode("utf-8") + body,
            hashlib.sha256,
        ).hexdigest()
        headers = {
            "X-Voice-Timestamp": timestamp,
            "X-Voice-Signature": signature,
            "Content-Type": "application/x-www-form-urlencoded",
        }
        first = client.post(
            f"/dispatch/voice/callback/{notification.id}",
            content=body,
            headers=headers,
        )
        assert first.status_code == 200
        first_ack_at = incident.acknowledged_at
        assert incident.status == models.IncidentStatus.ACKNOWLEDGED
        assert incident.acknowledged_by == notification.contact_id
        assert notification.status == models.NotificationStatus.ANSWERED_VOICE
        assert first_ack_at is not None

        second = client.post(
            f"/dispatch/voice/callback/{notification.id}",
            content=body,
            headers=headers,
        )
        assert second.status_code == 200
        assert incident.status == models.IncidentStatus.ACKNOWLEDGED
        assert incident.acknowledged_by == notification.contact_id
        assert incident.acknowledged_at == first_ack_at
        assert notification.status == models.NotificationStatus.ANSWERED_VOICE
        assert (
            sum(
                1
                for obj in fake_session.added_objects
                if isinstance(obj, models.AuditLog)
                and obj.action == models.AuditAction.ACK_RECEIVED
            )
            == 1
        )
    finally:
        main.app.dependency_overrides.pop(main.get_db, None)


def test_generate_prerecorded_twiml_success(client):
    incident = IncidentFixture(
        id="00000000-0000-0000-0000-00000000c111",
        external_event_id="evt-voice-pre-1",
        source="pagerduty",
        severity="CRITICAL",
        title="Voice pre-recorded",
        message="m",
        payload_json={"k": "v"},
        status=models.IncidentStatus.OPEN,
        matched_rule_id=None,
        dedupe_key=None,
        created_at=datetime(2026, 3, 20, 14, 30, 0),
    )
    notification = NotificationFixture(
        id="00000000-0000-0000-0000-00000000c211",
        incident_id=incident.id,
        contact_id="00000000-0000-0000-0000-00000000c311",
        channel=models.NotificationChannel.VOICE,
        status=models.NotificationStatus.SENT,
    )
    fake_session = FakeVoiceSession([incident], [notification])
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    original_audio_url = main.VOICE_PRERECORDED_AUDIO_URL
    main.VOICE_PRERECORDED_AUDIO_URL = "https://cdn.example.com/incident-alert.mp3"
    try:
        response = client.get(f"/dispatch/voice/twiml/prerecorded/{notification.id}")
        assert response.status_code == 200
        payload = response.text
        assert "<Play>https://cdn.example.com/incident-alert.mp3</Play>" in payload
        assert (
            f'action="{main.APP_BASE_URL}/dispatch/voice/callback/{notification.id}"'
            in payload
        )
        assert any(
            isinstance(obj, models.AuditLog)
            and obj.action == models.AuditAction.TWIML_GENERATED
            and obj.details_json.get("mode") == "pre_recorded"
            for obj in fake_session.added_objects
        )
    finally:
        main.VOICE_PRERECORDED_AUDIO_URL = original_audio_url
        main.app.dependency_overrides.pop(main.get_db, None)


def test_generate_prerecorded_twiml_requires_audio_url(client):
    original_audio_url = main.VOICE_PRERECORDED_AUDIO_URL
    main.VOICE_PRERECORDED_AUDIO_URL = ""
    try:
        response = client.get(
            "/dispatch/voice/twiml/prerecorded/00000000-0000-0000-0000-00000000c211"
        )
        assert response.status_code == 503
        assert response.json()["detail"] == "Pre-recorded audio URL is not configured"
    finally:
        main.VOICE_PRERECORDED_AUDIO_URL = original_audio_url


def test_get_incident_timeline_success(client):
    incident_id = uuid.UUID("00000000-0000-0000-0000-00000000d101")
    incident = IncidentFixture(
        id=str(incident_id),
        external_event_id="evt-time-1",
        source="prometheus",
        severity="CRITICAL",
        title="Timeline incident",
        message="m",
        payload_json={"k": "v"},
        status=models.IncidentStatus.OPEN,
        matched_rule_id=None,
        dedupe_key=None,
        created_at=datetime(2026, 3, 20, 9, 0, 0),
    )
    audit_logs = [
        models.AuditLog(
            id=uuid.UUID("00000000-0000-0000-0000-00000000d201"),
            trace_id="trace-1",
            incident_id=incident_id,
            action=models.AuditAction.TASK_QUEUED,
            details_json={"step": 2},
            created_at=datetime(2026, 3, 20, 9, 10, 0),
        ),
        models.AuditLog(
            id=uuid.UUID("00000000-0000-0000-0000-00000000d202"),
            trace_id="trace-2",
            incident_id=incident_id,
            action=models.AuditAction.EVENT_RECEIVED,
            details_json={"step": 1},
            created_at=datetime(2026, 3, 20, 9, 5, 0),
        ),
        models.AuditLog(
            id=uuid.UUID("00000000-0000-0000-0000-00000000d203"),
            trace_id="trace-3",
            incident_id=incident_id,
            action=models.AuditAction.CALLBACK_RECEIVED,
            details_json={"step": 3},
            created_at=datetime(2026, 3, 20, 9, 20, 0),
        ),
    ]
    fake_session = FakeLifecycleSession([incident], audit_logs=audit_logs)
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    try:
        response = client.get(f"/incidents/{incident.id}/timeline?limit=2&offset=0")
        assert response.status_code == 200
        payload = response.json()
        assert payload["total"] == 3
        assert payload["limit"] == 2
        assert payload["offset"] == 0
        assert [item["id"] for item in payload["items"]] == [
            "00000000-0000-0000-0000-00000000d202",
            "00000000-0000-0000-0000-00000000d201",
        ]
        assert [item["action"] for item in payload["items"]] == [
            "EVENT_RECEIVED",
            "TASK_QUEUED",
        ]
    finally:
        main.app.dependency_overrides.pop(main.get_db, None)


def test_get_incident_timeline_filter_action(client):
    incident_id = uuid.UUID("00000000-0000-0000-0000-00000000d301")
    incident = IncidentFixture(
        id=str(incident_id),
        external_event_id="evt-time-2",
        source="grafana",
        severity="WARN",
        title="Timeline incident filter",
        message=None,
        payload_json=None,
        status=models.IncidentStatus.OPEN,
        matched_rule_id=None,
        dedupe_key=None,
        created_at=datetime(2026, 3, 20, 10, 0, 0),
    )
    audit_logs = [
        models.AuditLog(
            id=uuid.UUID("00000000-0000-0000-0000-00000000d301"),
            trace_id="trace-4",
            incident_id=incident_id,
            action=models.AuditAction.EVENT_RECEIVED,
            details_json={"step": 1},
            created_at=datetime(2026, 3, 20, 10, 1, 0),
        ),
        models.AuditLog(
            id=uuid.UUID("00000000-0000-0000-0000-00000000d302"),
            trace_id="trace-5",
            incident_id=incident_id,
            action=models.AuditAction.TASK_QUEUED,
            details_json={"step": 2},
            created_at=datetime(2026, 3, 20, 10, 2, 0),
        ),
        models.AuditLog(
            id=uuid.UUID("00000000-0000-0000-0000-00000000d303"),
            trace_id="trace-6",
            incident_id=incident_id,
            action=models.AuditAction.EVENT_RECEIVED,
            details_json={"step": 3},
            created_at=datetime(2026, 3, 20, 10, 3, 0),
        ),
    ]
    fake_session = FakeLifecycleSession([incident], audit_logs=audit_logs)
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    try:
        response = client.get(
            f"/incidents/{incident.id}/timeline?action=EVENT_RECEIVED&limit=10&offset=0"
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["total"] == 2
        assert [item["action"] for item in payload["items"]] == [
            "EVENT_RECEIVED",
            "EVENT_RECEIVED",
        ]
        assert [item["id"] for item in payload["items"]] == [
            "00000000-0000-0000-0000-00000000d301",
            "00000000-0000-0000-0000-00000000d303",
        ]
    finally:
        main.app.dependency_overrides.pop(main.get_db, None)


def test_get_incident_timeline_limit_above_max(client):
    incident_id = uuid.UUID("00000000-0000-0000-0000-00000000d401")
    incident = IncidentFixture(
        id=str(incident_id),
        external_event_id="evt-time-4",
        source="prometheus",
        severity="CRITICAL",
        title="Limit incident",
        message=None,
        payload_json=None,
        status=models.IncidentStatus.OPEN,
        matched_rule_id=None,
        dedupe_key=None,
        created_at=datetime(2026, 3, 20, 11, 0, 0),
    )
    fake_session = FakeLifecycleSession([incident], audit_logs=[])
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    try:
        response = client.get(
            f"/incidents/{incident.id}/timeline?limit={main.OPS_ENDPOINT_MAX_LIMIT + 1}"
        )
        assert response.status_code == 400
    finally:
        main.app.dependency_overrides.pop(main.get_db, None)


def test_get_incident_timeline_not_found(client):
    fake_session = FakeLifecycleSession([], audit_logs=[])
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    try:
        response = client.get(
            "/incidents/00000000-0000-0000-0000-00000000ffff/timeline"
        )
        assert response.status_code == 404
    finally:
        main.app.dependency_overrides.pop(main.get_db, None)


def test_list_rules_filters_and_pagination(client):
    group_a = GroupFixture(id="00000000-0000-0000-0000-00000000a001", name="group-a")
    group_b = GroupFixture(id="00000000-0000-0000-0000-00000000b001", name="group-b")
    rules = [
        RuleFixture(
            id="00000000-0000-0000-0000-000000000101",
            rule_name="rule-1",
            condition_json={"source": "prometheus"},
            recipient_group_id=group_a.id,
            channels=["voice"],
            active=True,
            priority=20,
            requires_ack=True,
            ack_deadline=300,
            fallback_policy_json=None,
        ),
        RuleFixture(
            id="00000000-0000-0000-0000-000000000102",
            rule_name="rule-2",
            condition_json={"source": "grafana"},
            recipient_group_id=group_a.id,
            channels=["telegram"],
            active=True,
            priority=10,
            requires_ack=False,
            ack_deadline=None,
            fallback_policy_json=None,
        ),
        RuleFixture(
            id="00000000-0000-0000-0000-000000000103",
            rule_name="rule-3",
            condition_json={"source": "grafana"},
            recipient_group_id=group_b.id,
            channels=["email"],
            active=False,
            priority=30,
            requires_ack=False,
            ack_deadline=None,
            fallback_policy_json=None,
        ),
    ]
    fake_session = FakeRuleSession(rules, [group_a, group_b])
    main.app.dependency_overrides[main.get_db] = lambda: fake_session

    response = client.get(
        f"/rules?active=true&recipient_group_id={group_a.id}&limit=1&offset=0"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    assert payload["limit"] == 1
    assert payload["offset"] == 0
    assert len(payload["items"]) == 1
    assert payload["items"][0]["id"] == "00000000-0000-0000-0000-000000000102"
    assert payload["items"][0]["priority"] == 10


def test_list_rules_limit_above_max(client):
    response = client.get(f"/rules?limit={main.OPS_ENDPOINT_MAX_LIMIT + 1}")
    assert response.status_code == 400


def test_create_rule_with_valid_fallback_policy(client):
    fake_session = FakeSession(queries=[])
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    try:
        response = client.post(
            "/rules",
            json={
                "rule_name": "rule-create",
                "condition_json": {"source": "prometheus"},
                "recipient_group_id": "00000000-0000-0000-0000-00000000aa01",
                "channels": ["voice"],
                "active": True,
                "priority": 25,
                "requires_ack": False,
                "ack_deadline": None,
                "fallback_policy_json": {
                    "escalation_group_id": "00000000-0000-0000-0000-00000000aa02",
                    "channels": ["telegram", "email"],
                },
            },
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["rule_name"] == "rule-create"
        assert (
            payload["fallback_policy_json"]["escalation_group_id"]
            == "00000000-0000-0000-0000-00000000aa02"
        )
        assert payload["fallback_policy_json"]["channels"] == ["telegram", "email"]
    finally:
        main.app.dependency_overrides.pop(main.get_db, None)


def test_update_rule_and_toggle(client):
    group_a = GroupFixture(id="00000000-0000-0000-0000-00000000c001", name="group-c")
    group_b = GroupFixture(id="00000000-0000-0000-0000-00000000d001", name="group-d")
    rule = RuleFixture(
        id="00000000-0000-0000-0000-000000000201",
        rule_name="rule-old",
        condition_json={"source": "old"},
        recipient_group_id=group_a.id,
        channels=["voice"],
        active=True,
        priority=50,
        requires_ack=False,
        ack_deadline=None,
        fallback_policy_json=None,
    )
    fake_session = FakeRuleSession([rule], [group_a, group_b])
    main.app.dependency_overrides[main.get_db] = lambda: fake_session

    response = client.patch(
        f"/rules/{rule.id}",
        json={
            "rule_name": "rule-new",
            "recipient_group_id": group_b.id,
            "channels": ["telegram", "email"],
            "active": False,
            "priority": 5,
            "requires_ack": True,
            "ack_deadline": 600,
            "fallback_policy_json": {
                "escalation_group_id": group_a.id,
                "channels": ["voice"],
            },
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["rule_name"] == "rule-new"
    assert payload["recipient_group_id"] == group_b.id
    assert payload["channels"] == ["telegram", "email"]
    assert payload["active"] is False
    assert payload["priority"] == 5
    assert payload["requires_ack"] is True
    assert payload["ack_deadline"] == 600
    assert payload["fallback_policy_json"]["escalation_group_id"] == group_a.id
    assert payload["fallback_policy_json"]["channels"] == ["voice"]

    toggled = client.post(f"/rules/{rule.id}/toggle")
    assert toggled.status_code == 200
    assert toggled.json()["active"] is True


@pytest.mark.parametrize(
    "payload, expected_fragment",
    [
        (
            {
                "rule_name": "rule-invalid-missing",
                "condition_json": {"source": "prometheus"},
                "recipient_group_id": "00000000-0000-0000-0000-00000000aa11",
                "channels": ["voice"],
                "fallback_policy_json": {"channels": ["voice"]},
            },
            "fallback_policy_json must include escalation_group_id and channels",
        ),
        (
            {
                "rule_name": "rule-invalid-empty-channels",
                "condition_json": {"source": "prometheus"},
                "recipient_group_id": "00000000-0000-0000-0000-00000000aa12",
                "channels": ["voice"],
                "fallback_policy_json": {
                    "escalation_group_id": "00000000-0000-0000-0000-00000000aa13",
                    "channels": [],
                },
            },
            "fallback_policy_json.channels must be a non-empty list of strings",
        ),
        (
            {
                "rule_name": "rule-invalid-uuid",
                "condition_json": {"source": "prometheus"},
                "recipient_group_id": "00000000-0000-0000-0000-00000000aa14",
                "channels": ["voice"],
                "fallback_policy_json": {
                    "escalation_group_id": "not-a-uuid",
                    "channels": ["voice"],
                },
            },
            "fallback_policy_json.escalation_group_id must be a valid UUID",
        ),
    ],
)
def test_create_rule_fallback_policy_validation_errors(
    client, payload, expected_fragment
):
    response = client.post("/rules", json=payload)
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert any(expected_fragment in item["msg"] for item in detail)


@pytest.mark.parametrize(
    "payload, expected_fragment",
    [
        (
            {"fallback_policy_json": {"channels": ["voice"]}},
            "fallback_policy_json must include escalation_group_id and channels",
        ),
        (
            {
                "fallback_policy_json": {
                    "escalation_group_id": "00000000-0000-0000-0000-00000000aa22",
                    "channels": [],
                }
            },
            "fallback_policy_json.channels must be a non-empty list of strings",
        ),
        (
            {
                "fallback_policy_json": {
                    "escalation_group_id": "not-a-uuid",
                    "channels": ["voice"],
                }
            },
            "fallback_policy_json.escalation_group_id must be a valid UUID",
        ),
    ],
)
def test_update_rule_fallback_policy_validation_errors(
    client, payload, expected_fragment
):
    group = GroupFixture(id="00000000-0000-0000-0000-00000000aa20", name="group-aa20")
    rule = RuleFixture(
        id="00000000-0000-0000-0000-000000000221",
        rule_name="rule",
        condition_json={"source": "x"},
        recipient_group_id=group.id,
        channels=["voice"],
        active=True,
        priority=10,
        requires_ack=False,
        ack_deadline=None,
        fallback_policy_json=None,
    )
    fake_session = FakeRuleSession([rule], [group])
    main.app.dependency_overrides[main.get_db] = lambda: fake_session
    try:
        response = client.patch(f"/rules/{rule.id}", json=payload)
        assert response.status_code == 422
        detail = response.json()["detail"]
        assert any(expected_fragment in item["msg"] for item in detail)
    finally:
        main.app.dependency_overrides.pop(main.get_db, None)


def test_update_rule_errors(client):
    group = GroupFixture(id="00000000-0000-0000-0000-00000000e001", name="group-e")
    rule = RuleFixture(
        id="00000000-0000-0000-0000-000000000301",
        rule_name="rule",
        condition_json={"source": "x"},
        recipient_group_id=group.id,
        channels=["voice"],
        active=True,
        priority=10,
        requires_ack=False,
        ack_deadline=None,
        fallback_policy_json=None,
    )
    fake_session = FakeRuleSession([rule], [group])
    main.app.dependency_overrides[main.get_db] = lambda: fake_session

    invalid_id = client.patch("/rules/not-a-uuid", json={"rule_name": "x"})
    assert invalid_id.status_code == 400

    not_found = client.patch(
        "/rules/00000000-0000-0000-0000-000000000999", json={"rule_name": "x"}
    )
    assert not_found.status_code == 404

    missing_group = client.patch(
        f"/rules/{rule.id}",
        json={"recipient_group_id": "00000000-0000-0000-0000-00000000ffff"},
    )
    assert missing_group.status_code == 404

    no_fields = client.patch(f"/rules/{rule.id}", json={})
    assert no_fields.status_code == 400

    toggle_not_found = client.post("/rules/00000000-0000-0000-0000-000000000999/toggle")
    assert toggle_not_found.status_code == 404


def test_simulate_rule_match(client):
    group = GroupFixture(id="00000000-0000-0000-0000-00000000f001", name="group-f")
    rule = RuleFixture(
        id="00000000-0000-0000-0000-000000000401",
        rule_name="simulate-rule",
        condition_json={"source": "prometheus", "severity": "CRITICAL"},
        recipient_group_id=group.id,
        channels=["voice"],
        active=True,
        priority=1,
        requires_ack=False,
        ack_deadline=None,
        fallback_policy_json=None,
    )
    fake_session = FakeRuleSession([rule], [group])
    main.app.dependency_overrides[main.get_db] = lambda: fake_session

    response = client.post(
        f"/rules/{rule.id}/simulate",
        json={"source": "prometheus", "severity": "CRITICAL", "host": "db01"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["rule_id"] == rule.id
    assert payload["matched"] is True
    assert payload["reasons"] == []
    assert payload["payload"]["host"] == "db01"


def test_simulate_rule_no_match(client):
    group = GroupFixture(id="00000000-0000-0000-0000-00000000f002", name="group-g")
    rule = RuleFixture(
        id="00000000-0000-0000-0000-000000000402",
        rule_name="simulate-rule-2",
        condition_json={"source": "prometheus", "severity": "CRITICAL"},
        recipient_group_id=group.id,
        channels=["voice"],
        active=True,
        priority=1,
        requires_ack=False,
        ack_deadline=None,
        fallback_policy_json=None,
    )
    fake_session = FakeRuleSession([rule], [group])
    main.app.dependency_overrides[main.get_db] = lambda: fake_session

    response = client.post(
        f"/rules/{rule.id}/simulate",
        json={"source": "grafana", "severity": "WARN"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["matched"] is False
    assert len(payload["reasons"]) == 2
    assert any("source" in reason for reason in payload["reasons"])
    assert any("severity" in reason for reason in payload["reasons"])


def test_simulate_rule_errors(client):
    group = GroupFixture(id="00000000-0000-0000-0000-00000000f003", name="group-h")
    rule = RuleFixture(
        id="00000000-0000-0000-0000-000000000403",
        rule_name="simulate-rule-3",
        condition_json={"source": "prometheus"},
        recipient_group_id=group.id,
        channels=["voice"],
        active=True,
        priority=1,
        requires_ack=False,
        ack_deadline=None,
        fallback_policy_json=None,
    )
    fake_session = FakeRuleSession([rule], [group])
    main.app.dependency_overrides[main.get_db] = lambda: fake_session

    invalid = client.post("/rules/not-a-uuid/simulate", json={"source": "prometheus"})
    assert invalid.status_code == 400

    not_found = client.post(
        "/rules/00000000-0000-0000-0000-000000000999/simulate",
        json={"source": "prometheus"},
    )
    assert not_found.status_code == 404
