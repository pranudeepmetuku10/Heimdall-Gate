"""Logging configuration.

Two formatters are supported via the `HEIMDALL_LOG_FORMAT` env var:
  - "console": human-readable, single line, suitable for `make ingest`
  - "json": newline-delimited JSON, suitable for piping into a log aggregator

The configuration is applied once via `configure_logging(settings)` called from
the ingestion entrypoint. Library code uses `logging.getLogger(__name__)`
without further setup.
"""

from __future__ import annotations

import datetime as dt
import logging
import sys
from typing import Any

import orjson

_STD_ATTRS = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "message", "taskName",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": dt.datetime.fromtimestamp(
                record.created, tz=dt.timezone.utc
            ).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in _STD_ATTRS or key.startswith("_"):
                continue
            payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return orjson.dumps(payload, default=str).decode()


class ConsoleFormatter(logging.Formatter):
    default_fmt = "%(asctime)s %(levelname)-5s %(name)s :: %(message)s"
    default_datefmt = "%Y-%m-%dT%H:%M:%S"

    def __init__(self) -> None:
        super().__init__(fmt=self.default_fmt, datefmt=self.default_datefmt)

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        extras = {
            k: v
            for k, v in record.__dict__.items()
            if k not in _STD_ATTRS and not k.startswith("_")
        }
        if extras:
            tail = " ".join(f"{k}={v}" for k, v in extras.items())
            return f"{base} {tail}"
        return base


def configure_logging(level: str, fmt: str) -> None:
    """Idempotent. Safe to call from a test fixture between cases."""
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)

    handler = logging.StreamHandler(stream=sys.stdout)
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(ConsoleFormatter())

    root.addHandler(handler)
    root.setLevel(level)

    # Tame noisy third-party loggers.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
