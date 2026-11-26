"""Mobility Metrics Calculator for GeoPulse.

Computes retail visit metrics from matched spatial pings:
- Contiguous dwell duration per device per store
- Filtering visits against minimum dwell threshold (e.g. 5 minutes)
- H3 hexagonal spatial aggregation
"""

from typing import Any, Dict, List, Optional
import pandas as pd

from src.common.logger import get_logger

logger = get_logger(__name__)


class MobilityMetricsCalculator:
    """Calculates visit intervals, dwell durations, and spatial visit counts."""

    def __init__(
        self,
        min_dwell_minutes: float = 5.0,
        max_dwell_minutes: float = 240.0,
        max_gap_seconds: float = 300.0,
        speed_threshold_mps: float = 1.5,
    ):
        """Initialize mobility metrics calculator.

        Args:
            min_dwell_minutes: Minimum dwell time in minutes for confirmed visit.
            max_dwell_minutes: Maximum plausible dwell time.
            max_gap_seconds: Session gap timeout in seconds.
            speed_threshold_mps: Speed limit for stationary dwell.
        """
        self.min_dwell_minutes = min_dwell_minutes
        self.max_dwell_minutes = max_dwell_minutes
        self.max_gap_seconds = max_gap_seconds
        self.speed_threshold_mps = speed_threshold_mps

    def calculate_dwell_times(self, matched_pings_df: Any) -> Any:
        """Group contiguous pings per device and compute dwell duration.

        Args:
            matched_pings_df: DataFrame of spatially joined pings containing
                              device_id, store_id, timestamp_utc, and optionally speed_mps.

        Returns:
            pandas.DataFrame containing sessionized dwell times and visit classification.
        """
        from src.member1_data_eng.spatial_processing.visit_detector import StoreVisitDetector

        detector = StoreVisitDetector(
            min_dwell_minutes=self.min_dwell_minutes,
            max_dwell_minutes=self.max_dwell_minutes,
            speed_threshold_mps=self.speed_threshold_mps,
            max_session_gap_seconds=self.max_gap_seconds,
        )
        events = detector.detect_continuous_events(matched_pings_df, max_gap_seconds=self.max_gap_seconds)
        events_df = detector.events_to_dataframe(events)
        return events_df

    def assign_h3_hexagons(self, df: Any, resolution: int = 9) -> Any:
        """Attach Uber H3 spatial index for hexagonal density clustering.

        Args:
            df: DataFrame containing latitude and longitude columns.
            resolution: H3 resolution level (default 9, ~100m edge length).

        Returns:
            DataFrame with 'h3_index' column attached.
        """
        try:
            import h3
        except ImportError:
            raise ImportError("h3 package is required for H3 spatial indexing.")

        res_df = df.copy()
        res_df["h3_index"] = res_df.apply(
            lambda r: h3.geo_to_h3(float(r["latitude"]), float(r["longitude"]), resolution),
            axis=1,
        )
        return res_df
