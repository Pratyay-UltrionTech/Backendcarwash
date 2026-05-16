from __future__ import annotations

import json
import logging
import time
from typing import Any

from fastapi import Request

_log = logging.getLogger("uvicorn.error")
_PII_KEYWORDS = ("phone", "email", "address", "name", "password", "token")


def configure_logging(log_level: str = "INFO") -> None:
    level_name = (log_level or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(name).setLevel(level)


def request_id_from(request: Request | None) -> str:
    if request is None:
        return "n/a"
    return getattr(request.state, "request_id", "n/a")


def _mask_value(key: str, value: Any) -> Any:
    key_l = key.lower()
    if not any(word in key_l for word in _PII_KEYWORDS):
        return value
    if value is None:
        return None
    if isinstance(value, str):
        if len(value) <= 4:
            return "***"
        return f"{value[:2]}***{value[-2:]}"
    return "***"


def _safe_fields(fields: dict[str, Any]) -> dict[str, Any]:
    return {k: _mask_value(k, v) for k, v in fields.items()}


def action_log(action: str, outcome: str, request: Request | None = None, **fields: Any) -> None:
    request_id = request_id_from(request)
    body = {"event": "action", "action": action, "outcome": outcome, "request_id": request_id, **_safe_fields(fields)}
    _log.info(json.dumps(body, default=str, separators=(",", ":")))


def audit_log(actor_type: str, actor_id: str, action: str, request: Request | None = None, **fields: Any) -> None:
    request_id = request_id_from(request)
    body = {
        "event": "audit",
        "actor_type": actor_type,
        "actor_id": _mask_value("actor_id", actor_id),
        "action": action,
        "request_id": request_id,
        **_safe_fields(fields),
    }
    _log.warning(json.dumps(body, default=str, separators=(",", ":")))


def monotonic_ms() -> float:
    return time.perf_counter() * 1000
