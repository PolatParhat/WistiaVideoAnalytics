"""JSON-lines logging: one JSON object per log line, easy to search in CloudWatch Logs Insights."""

import json
import logging
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    def format(self, record):
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        entry.update(getattr(record, "fields", {}))
        if record.exc_info:
            entry["error"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def get_logger(name):
    """Return a logger that prints JSON lines to stdout (Lambda and Glue send stdout to CloudWatch)."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False  # Lambda adds its own root handler; avoid double lines
    return logger


def log_event(logger, msg, level=logging.INFO, **fields):
    """Log a message with structured fields: log_event(log, "api call", status=200)."""
    logger.log(level, msg, extra={"fields": fields})
