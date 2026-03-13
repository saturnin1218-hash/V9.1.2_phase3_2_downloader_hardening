from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional


def setup_logger(name: str = "binance_quant_builder", log_file: Optional[str] = None, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False
    if not logger.handlers:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(stream_handler)
    if log_file:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        existing = {getattr(h, "baseFilename", None) for h in logger.handlers}
        if str(path) not in existing:
            file_handler = logging.FileHandler(path, encoding="utf-8")
            file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
            logger.addHandler(file_handler)
    return logger
