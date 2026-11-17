"""FastAPI backend application for GeoPulse.

Serves retail store polygons, spatial metrics, dwell analytics,
and Kepler.gl configuration to the React frontend dashboard.

Status: Staged for Day 9 implementation.
"""

from typing import Any, Dict
from src.common.config_loader import load_yaml_config

try:
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware

    app = FastAPI(
        title="GeoPulse Mobility Analytics API",
        version="0.1.0",
        description="REST API serving hyper-local retail mobility analytics and GeoJSON boundaries.",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/health")
    def health_check() -> Dict[str, str]:
        """Health check endpoint."""
        return {"status": "healthy", "service": "GeoPulse API"}

    @app.get("/api/v1/study-zone")
    def get_study_zone() -> Dict[str, Any]:
        """Return study zone configuration."""
        cfg = load_yaml_config()
        return cfg.get("study_zone", {})

except ImportError:
    # Minimal stub when fastapi is not yet installed
    app = None  # type: ignore
