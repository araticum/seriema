from __future__ import annotations

import json
import re
from typing import Any

SeverityTitle = tuple[str, str]

_JSON_LEVEL_SEVERITY = {
    "trace": "INFO",
    "debug": "INFO",
    "info": "INFO",
    "information": "INFO",
    "warn": "WARN",
    "warning": "WARN",
    "error": "ERROR",
    "err": "ERROR",
    "critical": "CRITICAL",
    "crit": "CRITICAL",
    "fatal": "FATAL",
    "panic": "FATAL",
    "emergency": "FATAL",
    "alert": "CRITICAL",
}


def _normalize_service(service: str, labels: dict[str, Any]) -> str:
    candidates = [
        service,
        str(labels.get("compose_service", "") or ""),
        str(labels.get("service", "") or ""),
        str(labels.get("app", "") or ""),
        str(labels.get("job", "") or ""),
        str(labels.get("container", "") or ""),
    ]
    joined = " ".join(part.lower() for part in candidates if part)

    if any(token in joined for token in ("postgres", "postgre", "monstro-postgres")):
        return "postgres"
    if any(token in joined for token in ("redis", "monstro-redis")):
        return "redis"
    if any(
        token in joined
        for token in ("seaweed", "weed", "sargaco", "sargaço", "monstro-sargaco")
    ):
        return "sargaco"
    if "seriema" in joined:
        if "beat" in joined:
            return "seriema-beat"
        if "worker" in joined:
            return "seriema-worker"
        if "api" in joined:
            return "seriema-api"
        return "seriema"
    if "loki" in joined:
        return "loki"
    if any(
        token in joined
        for token in (
            "celery",
            "ingest",
            "download",
            "parser",
            "compendio",
            "compêndio",
        )
    ):
        if "ingest" in joined:
            return "celery-ingest"
        if "download" in joined:
            return "celery-download"
        if "parser" in joined:
            return "celery-parser"
        if "compendio" in joined or "compêndio" in joined:
            return "celery-compendio"
        return "celery"
    if "veredas" in joined:
        return "veredas"
    if "ariranha" in joined:
        return "ariranha"
    if "candeia" in joined:
        return "candeia"
    return (service or "oasis-radar").strip().lower() or "oasis-radar"


def _parsed_json(log_line: str) -> dict[str, Any] | None:
    if not log_line.startswith("{"):
        return None
    try:
        parsed = json.loads(log_line)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def _severity_from_json(parsed: dict[str, Any]) -> str | None:
    for key in ("severity", "level", "log_level", "status"):
        value = parsed.get(key)
        if not value:
            continue
        mapped = _JSON_LEVEL_SEVERITY.get(str(value).strip().lower())
        if mapped:
            return mapped
    return None


def _title_from_text(prefix: str, line: str) -> str:
    compact = re.sub(r"\s+", " ", line).strip().strip('"')
    if not compact:
        compact = "empty log line"
    if len(compact) > 120:
        compact = f"{compact[:117]}..."
    return f"{prefix}: {compact}"


def _classify_postgres(line: str) -> SeverityTitle:
    lower = line.lower()

    if "password authentication failed" in lower:
        return "ERROR", "Postgres authentication failed"
    if "too many connections for role" in lower:
        return "ERROR", "Postgres connection limit reached"
    if "canceling statement due to lock timeout" in lower:
        return "ERROR", "Postgres lock timeout"

    duration_match = re.search(r"duration:\s*(\d+(?:\.\d+)?)\s*ms", lower)
    if duration_match and float(duration_match.group(1)) > 5000:
        return "ERROR", "Postgres slow query"

    if "not yet accepting connections" in lower:
        return "CRITICAL", "Postgres not accepting connections"
    if "could not extend file" in lower or "could not write to file" in lower:
        return "CRITICAL", "Postgres storage write failure"

    if "invalid page in block" in lower or "page verification failed" in lower:
        return "FATAL", "Postgres data corruption detected"
    if "could not open relation mapping file" in lower:
        return "FATAL", "Postgres relation mapping failure"
    if "database system is shut down" in lower:
        return "FATAL", "Postgres is shut down"
    if "panic:" in lower:
        return "FATAL", "Postgres panic"

    return _default_text_classification("Postgres log", line)


def _classify_redis(line: str) -> SeverityTitle:
    lower = line.lower()

    if "server initialized" in lower:
        return "INFO", "Redis server initialized"
    if "maxmemory" in lower or "keys_evicted" in lower:
        return "ERROR", "Redis memory pressure"
    if "latency latest" in lower:
        return "ERROR", "Redis high latency"
    if "background aof rewrite" in lower and any(
        token in lower for token in ("error", "failed", "fail")
    ):
        return "CRITICAL", "Redis AOF rewrite failed"
    if "redis is down" in lower:
        return "FATAL", "Redis is down"
    if "out of memory" in lower or "oom-killer" in lower:
        return "FATAL", "Redis killed by OOM"

    return _default_text_classification("Redis log", line)


def _classify_celery(service: str, line: str) -> SeverityTitle:
    lower = line.lower()
    worker_name = service.removeprefix("celery-") if service != "celery" else "worker"

    if "retry in" in lower or "retrying in" in lower:
        return "ERROR", f"Celery {worker_name} task retry scheduled"
    if "maxretriesexceedederror" in lower or "max retries" in lower:
        return "ERROR", f"Celery {worker_name} task exhausted retries"
    if "softtimelimitexceeded" in lower or "timelimitexceeded" in lower:
        return "ERROR", f"Celery {worker_name} task time limit exceeded"
    if (
        "cannot connect to redis://" in lower
        or "broker transport" in lower
        and "unreachable" in lower
    ):
        return "CRITICAL", f"Celery {worker_name} lost broker connectivity"
    if "restart loop" in lower or "restartcount" in lower:
        return "CRITICAL", f"Celery {worker_name} restart loop detected"
    if "no workers available" in lower or "no consumer" in lower:
        return "CRITICAL", f"Celery {worker_name} queue has no consumer"

    return _default_text_classification(f"Celery {worker_name} log", line)


def _classify_sargaco(line: str) -> SeverityTitle:
    lower = line.lower()
    if "s3error" in lower or "connectionerror" in lower and "retry" in lower:
        return "ERROR", "Sargaço transient storage failure"
    if "slow request" in lower:
        return "ERROR", "Sargaço slow request"
    if "no writable volumes" in lower or "disk full" in lower:
        return "CRITICAL", "Sargaço out of writable storage"
    if "connectionrefusederror" in lower or "timeout" in lower and "8333" in lower:
        return "CRITICAL", "Sargaço S3 endpoint unavailable"
    if "weed server" in lower and any(
        token in lower for token in ("exit", "dead", "not present")
    ):
        return "FATAL", "Sargaço process exited"

    return _default_text_classification("Sargaço log", line)


def _classify_seriema(service: str, line: str, labels: dict[str, Any]) -> SeverityTitle:
    lower = line.lower()

    if "telegram.error" in lower or "httperror 429" in lower and "retry" in lower:
        return "ERROR", "Seriema notification retry"
    if "dlq_replay_partial_failure" in lower:
        return "ERROR", "Seriema DLQ replay partial failure"
    if "oasis_radar:pull_failures" in lower:
        match = re.search(r"pull_failures\s*(?:>=|=|:)\s*(\d+)", lower)
        failures = int(match.group(1)) if match else None
        if failures is not None and failures >= 3:
            return "CRITICAL", "Seriema Oasis Radar pull failing repeatedly"
        return "ERROR", "Seriema Oasis Radar pull failure"
    if "cannot connect to redis://" in lower:
        return "CRITICAL", "Seriema worker lost broker connectivity"
    if "heartbeat" in lower:
        age_candidates = [
            labels.get("heartbeat_age_seconds"),
            labels.get("seconds_since_heartbeat"),
            labels.get("heartbeat_age"),
        ]
        for value in age_candidates:
            try:
                if value is not None and float(value) > 300:
                    return "CRITICAL", "Seriema worker heartbeat missing"
            except (TypeError, ValueError):
                pass
        heartbeat_match = re.search(
            r"(\d+)\s*(?:s|sec|secs|seconds|min|mins|minutes)", lower
        )
        if heartbeat_match:
            amount = int(heartbeat_match.group(1))
            if "min" in lower and amount >= 5:
                return "CRITICAL", "Seriema worker heartbeat missing"
            if (
                any(token in lower for token in ("second", "sec", "secs", "s"))
                and amount > 300
            ):
                return "CRITICAL", "Seriema worker heartbeat missing"
        if any(
            token in lower
            for token in ("missing heartbeat", "heartbeat overdue", "heartbeat stale")
        ):
            return "CRITICAL", "Seriema worker heartbeat missing"
    if all(token in lower for token in ("api", "worker", "beat")) and any(
        token in lower for token in ("dead", "down", "exit", "stopped")
    ):
        return "FATAL", "Seriema fully unavailable"

    label_role = (
        "worker"
        if "worker" in service
        else "api"
        if "api" in service
        else "beat"
        if "beat" in service
        else "service"
    )
    return _default_text_classification(f"Seriema {label_role} log", line)


def _classify_loki(line: str) -> SeverityTitle:
    lower = line.lower()

    if (
        "at least one label pair" in lower
        or "error processing query" in lower
        or (
            "promtail" in lower
            and any(token in lower for token in ("batch", "invalid", "label pair"))
        )
    ):
        return "INFO", "Loki: batch processing noise (ignore)"

    return _default_text_classification("Loki log", line)


def _split_prefixed_log_line(line: str) -> tuple[str | None, str]:
    match = re.match(r"^(INFO|WARNING|WARN|ERROR|CRITICAL|DEBUG):\s*(.*)$", line)
    if not match:
        return None, line
    return match.group(1), match.group(2)


def _extract_embedded_json(line: str) -> dict[str, Any] | None:
    start = line.find("{")
    if start < 0:
        return None
    try:
        parsed = json.loads(line[start:])
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def _classify_veredas(line: str) -> SeverityTitle:
    prefix, remainder = _split_prefixed_log_line(line)
    if prefix == "INFO":
        return "INFO", "Veredas info log"
    if prefix in {"WARNING", "WARN"}:
        return "WARN", "Veredas warning log"

    parsed = _extract_embedded_json(remainder if prefix else line)
    message = remainder if prefix else line
    lower = message.lower()

    if parsed is not None:
        json_severity = _severity_from_json(parsed)
        if json_severity == "INFO":
            return "INFO", "Veredas info log"
        if json_severity == "WARN":
            return "WARN", "Veredas warning log"
        message = str(
            parsed.get("message")
            or parsed.get("msg")
            or parsed.get("event")
            or parsed.get("detail")
            or remainder
            or line
        )
        lower = message.lower()

    if "database" in lower and (
        "does not exist" in lower or "connection refused" in lower
    ):
        return "CRITICAL", "Veredas: banco indisponível"
    if "celery" in lower and "connection refused" in lower:
        return "CRITICAL", "Veredas: Redis/broker indisponível"
    if "pncp" in lower and ("timeout" in lower or "failed" in lower):
        return "ERROR", "Veredas: falha ingest PNCP"
    if "parser" in lower and "failed" in lower:
        return "ERROR", "Veredas: falha no parser de documentos"
    if "worker" in lower and ("unregistered" in lower or "not found" in lower):
        return "ERROR", "Veredas: Celery task não registrada"

    if prefix == "CRITICAL":
        return "CRITICAL", _title_from_text("Veredas log", remainder or line)
    if prefix == "ERROR":
        return "ERROR", _title_from_text("Veredas log", remainder or line)

    if parsed is not None:
        json_severity = _severity_from_json(parsed)
        if json_severity:
            return json_severity, _title_from_text("Veredas log", message)

    return _default_text_classification("Veredas log", remainder if prefix else line)


def _default_text_classification(prefix: str, line: str) -> SeverityTitle:
    lower = line.lower()
    if re.search(r"\b(panic|fatal)\b", lower):
        return "FATAL", _title_from_text(prefix, line)
    if re.search(r"\b(critical|crit)\b", lower):
        return "CRITICAL", _title_from_text(prefix, line)
    if re.search(r"\b(error|exception|failed|failure)\b", lower):
        return "ERROR", _title_from_text(prefix, line)
    if re.search(r"\b(warn|warning)\b", lower):
        return "WARN", _title_from_text(prefix, line)
    return "INFO", _title_from_text(prefix, line)


def classify_log_line(
    service: str, log_line: str, labels: dict[str, Any]
) -> SeverityTitle:
    normalized_labels = labels if isinstance(labels, dict) else {}
    line = (log_line or "").strip()
    normalized_service = _normalize_service(service, normalized_labels)
    parsed = _parsed_json(line)

    if parsed is not None:
        message = str(
            parsed.get("message")
            or parsed.get("msg")
            or parsed.get("event")
            or parsed.get("detail")
            or line
        )
        severity = _severity_from_json(parsed)
        title = str(parsed.get("title") or parsed.get("event") or message).strip()
        if normalized_service.startswith("seriema"):
            derived_severity, derived_title = _classify_seriema(
                normalized_service, message, {**normalized_labels, **parsed}
            )
            return derived_severity, title or derived_title
        if severity:
            return severity, title or _title_from_text(
                f"{normalized_service} log", message
            )
        line = message

    if normalized_service == "postgres":
        return _classify_postgres(line)
    if normalized_service == "redis":
        return _classify_redis(line)
    if normalized_service == "loki":
        return _classify_loki(line)
    if normalized_service in {"sargaco"}:
        return _classify_sargaco(line)
    if normalized_service == "veredas":
        return _classify_veredas(line)
    if normalized_service.startswith("celery"):
        return _classify_celery(normalized_service, line)
    if normalized_service.startswith("seriema"):
        return _classify_seriema(normalized_service, line, normalized_labels)

    return _default_text_classification(f"{normalized_service} log", line)
