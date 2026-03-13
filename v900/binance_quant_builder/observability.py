from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        extra = getattr(record, "event_payload", None)
        if isinstance(extra, dict):
            payload.update(extra)
        return json.dumps(payload, ensure_ascii=False, default=str)


def setup_run_logger(run_id: str, out_dir: str | Path, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(f"binance_quant_builder.{run_id}")
    logger.setLevel(level)
    logger.propagate = False
    if logger.handlers:
        return logger
    path = Path(out_dir) / "run.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(path, encoding="utf-8")
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    stream = logging.StreamHandler()
    stream.setFormatter(JsonFormatter())
    logger.addHandler(stream)
    return logger


def log_event(logger: logging.Logger, level: int, event: str, **payload: Any) -> None:
    logger.log(level, event, extra={"event_payload": {"event": event, **payload}})
