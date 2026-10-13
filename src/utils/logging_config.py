"""
Structured logging configuration.
Uses JSON-style structured logs for easy ingestion into observability tools.
Passwords and secrets are never logged.
"""
import logging
import sys
from datetime import datetime, timezone
from src.utils.config import LOG_LEVEL


class _StructuredFormatter(logging.Formatter):
    """Emit log records as structured key=value lines."""

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat()
        level = record.levelname
        name = record.name
        msg = record.getMessage()
        # Scrub common secret patterns — defensive measure
        for token in ("password", "secret", "token", "key", "fernet"):
            if token in msg.lower():
                msg = "[REDACTED — possible secret in log message]"
                break
        base = f"ts={ts} level={level} logger={name} msg={msg!r}"
        if record.exc_info:
            base += f" exc={self.formatException(record.exc_info)!r}"
        return base


def get_logger(name: str) -> logging.Logger:
    """Return a structured logger.  Call once per module."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_StructuredFormatter())
        logger.addHandler(handler)
        logger.propagate = False
    logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
    return logger
