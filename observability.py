import logging
from typing import Any, Literal

from .config import (
    LANGFUSE_HOST,
    LANGFUSE_PUBLIC_KEY,
    LANGFUSE_SECRET_KEY,
    SENTRY_DSN,
    SENTRY_ENVIRONMENT,
    SENTRY_RELEASE,
)

LogLevel = Literal["debug", "info", "warning", "error", "critical"]

logger = logging.getLogger("seriema.observability")

_sentry_sdk = None
_langfuse_client = None


def init_observability() -> None:
    global _sentry_sdk, _langfuse_client

    if SENTRY_DSN:
        try:
            import sentry_sdk  # type: ignore

            sentry_sdk.init(
                dsn=SENTRY_DSN,
                environment=SENTRY_ENVIRONMENT,
                release=SENTRY_RELEASE or None,
                traces_sample_rate=0.0,
            )
            _sentry_sdk = sentry_sdk
        except Exception as exc:
            logger.warning("Failed to initialize Sentry: %s", exc)

    if LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY:
        try:
            from langfuse import Langfuse  # type: ignore

            _langfuse_client = Langfuse(
                public_key=LANGFUSE_PUBLIC_KEY,
                secret_key=LANGFUSE_SECRET_KEY,
                host=LANGFUSE_HOST,
            )
        except Exception as exc:
            logger.warning("Failed to initialize Langfuse: %s", exc)


def _normalize_langfuse_trace_id(trace_id: str | None) -> str | None:
    if not trace_id:
        return None
    normalized = trace_id.replace("-", "")
    return normalized or None


def _send_langfuse_event(
    name: str,
    payload: dict[str, Any],
    level: LogLevel,
    trace_id: str | None = None,
) -> None:
    if _langfuse_client is None:
        return

    try:
        langfuse_level = level.upper()
        normalized_trace_id = _normalize_langfuse_trace_id(trace_id)

        if normalized_trace_id:
            with _langfuse_client.start_as_current_observation(
                name="seriema.event",
                as_type="span",
                trace_context={"trace_id": normalized_trace_id},
                metadata={"source": "seriema", "event_name": name},
                level=langfuse_level,
            ) as trace_span:
                trace_span.start_observation(
                    name=name,
                    as_type="event",
                    metadata=payload,
                    level=langfuse_level,
                )
        else:
            with _langfuse_client.start_as_current_observation(
                name="seriema.event",
                as_type="span",
                metadata={"source": "seriema", "event_name": name},
                level=langfuse_level,
            ) as trace_span:
                trace_span.start_observation(
                    name=name,
                    as_type="event",
                    metadata=payload,
                    level=langfuse_level,
                )

        if hasattr(_langfuse_client, "flush"):
            _langfuse_client.flush()
    except Exception as exc:
        logger.warning("Failed to send Langfuse event: %s", exc)


def notify_event(
    name: str,
    payload: dict[str, Any],
    level: LogLevel = "info",
    trace_id: str | None = None,
) -> None:
    if _sentry_sdk is not None:
        try:
            with _sentry_sdk.push_scope() as scope:
                scope.set_tag("component", "seriema")
                if trace_id:
                    scope.set_tag("trace_id", trace_id)
                scope.set_context("seriema_event", payload)
                _sentry_sdk.capture_message(name, level=level)
        except Exception as exc:
            logger.warning("Failed to send Sentry message: %s", exc)

    log_func = getattr(logger, level.lower(), logger.info)
    import json

    json_log = {
        "event_name": name,
        "trace_id": trace_id,
        "payload": payload,
        "component": "seriema",
    }
    log_func(json.dumps(json_log))

    _send_langfuse_event(name=name, payload=payload, level=level, trace_id=trace_id)


def notify_exception(
    error: Exception,
    payload: dict[str, Any],
    trace_id: str | None = None,
) -> None:
    if _sentry_sdk is not None:
        try:
            with _sentry_sdk.push_scope() as scope:
                scope.set_tag("component", "seriema")
                if trace_id:
                    scope.set_tag("trace_id", trace_id)
                scope.set_context("seriema_error", payload)
                _sentry_sdk.capture_exception(error)
        except Exception as exc:
            logger.warning("Failed to send Sentry exception: %s", exc)

    merged_payload = {"error": str(error), **payload}

    import json

    json_log = {
        "event_name": "seriema.exception",
        "trace_id": trace_id,
        "payload": merged_payload,
        "component": "seriema",
    }
    logger.error(json.dumps(json_log))

    _send_langfuse_event(
        name="seriema.exception",
        payload=merged_payload,
        level="error",
        trace_id=trace_id,
    )
