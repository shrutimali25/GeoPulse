"""Structured logging utility for GeoPulse."""

import os
import sys
import logging

try:
    from loguru import logger

    # Configure Loguru
    log_level = os.getenv("LOG_LEVEL", "INFO")
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=log_level,
    )
except ImportError:
    # Standard library fallback
    logger = logging.getLogger("geopulse")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


def get_logger(name: str):
    """Retrieve named logger instance."""
    return logger
