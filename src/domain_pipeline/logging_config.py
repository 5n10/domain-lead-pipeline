"""Structured JSON logging with correlation IDs.

Usage:
    from domain_pipeline.logging_config import setup_logging
    setup_logging()  # Call once at startup

Every log record gets a correlation_id that is either:
- pulled from the current request context (for API calls), or
- generated fresh (for background workers).
"""
from __future__ import annotations

import json
import logging
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Optional

# ContextVar holds the current correlation ID (set per request or worker batch)
correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="")


def get_correlation_id() -> str:
    """Return the current correlation ID, generating one if not set."""
    cid = correlation_id_var.get()
    if not cid:
        cid = uuid.uuid4().hex[:12]
        correlation_id_var.set(cid)
    return cid


def set_correlation_id(cid: Optional[str] = None) -> str:
    """Set (or generate) a correlation ID for the current context."""
    cid = cid or uuid.uuid4().hex[:12]
    correlation_id_var.set(cid)
    return cid


class JSONFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "correlation_id": get_correlation_id(),
        }
        if record.exc_info and record.exc_info[1]:
            log_entry["exception"] = self.formatException(record.exc_info)
        # Include extra fields passed via `extra={}` in log calls
        for key in ("job_name", "business_id", "worker", "batch_size", "duration_ms"):
            val = getattr(record, key, None)
            if val is not None:
                log_entry[key] = val
        return json.dumps(log_entry, default=str)


def setup_logging(level: str = "INFO", json_format: bool = True) -> None:
    """Configure the root logger with structured JSON output.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR).
        json_format: If True, use JSON formatter. If False, use standard format.
    """
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Remove existing handlers to avoid duplicates
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    handler = logging.StreamHandler()
    if json_format:
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s (%(correlation_id)s) %(message)s",
            defaults={"correlation_id": ""},
        ))
    root.addHandler(handler)

    # Quiet noisy libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
