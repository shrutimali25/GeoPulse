"""Configuration loader for GeoPulse."""

import os
from pathlib import Path
from typing import Any, Dict
import yaml

from src.common.logger import get_logger

logger = get_logger(__name__)


def get_project_root() -> Path:
    """Return the absolute path to the project root directory."""
    return Path(__file__).resolve().parent.parent.parent


def load_yaml_config(relative_path: str = "config/base.yaml") -> Dict[str, Any]:
    """Load and parse a YAML configuration file relative to the project root.

    Args:
        relative_path: Path relative to repository root.

    Returns:
        Parsed configuration dictionary.

    Raises:
        FileNotFoundError: If configuration file is missing.
    """
    root = get_project_root()
    config_file = root / relative_path

    if not config_file.exists():
        logger.error(f"Configuration file not found: {config_file}")
        raise FileNotFoundError(f"Missing config at: {config_file}")

    with open(config_file, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    logger.debug(f"Loaded configuration from {config_file}")
    return data or {}
