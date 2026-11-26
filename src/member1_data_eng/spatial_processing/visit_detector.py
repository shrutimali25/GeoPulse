"""Store Proximity & Visit Event Detection Pipeline for GeoPulse.

Performs spatial joins between GPS trajectory points and 500m store catchment polygons,
sessionizes continuous temporal intervals, computes dwell durations, and clearly
distinguishes broad catchment presence from confirmed physical retail visits.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from src.common.config_loader import get_project_root, load_yaml_config
from src.common.logger import get_logger
from src.member1_data_eng.spatial_processing.sedona_spatial_joins import (
    GEOPANDAS_AVAILABLE,
    SedonaSpatialJoinEngine,
)

logger = get_logger(__name__)

# Check pandas / geopandas availability
try:
    import geopandas as gpd
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = Any   # type: ignore
    gpd = Any  # type: ignore

# Default visit detection thresholds (aligned with config/base.yaml)
DEFAULT_MIN_DWELL_MINUTES = 5.0        # Confirmed retail visit threshold
DEFAULT_MAX_DWELL_MINUTES = 240.0      # 4 hours maximum plausible dwell
DEFAULT_SPEED_THRESHOLD_KMH = 3.0      # 3.0 km/h ~ 0.833 m/s indoor speed
DEFAULT_SPEED_THRESHOLD_MPS = 1.5      # Maximum pedestrian walking speed for dwell
DEFAULT_MAX_SESSION_GAP_SECONDS = 300  # 5 minutes without pings breaks continuity
DEFAULT_MIN_VISIT_PINGS = 3           # Minimum consecutive pings for confirmed visit


@dataclass
class StoreVisitEvent:
    """Represents a detected continuous store catchment presence or physical visit."""

    event_id: str
    device_id: str
    trajectory_id: str
    store_id: str
    store_name: str
    store_category: str
    entry_timestamp_utc: str
    exit_timestamp_utc: str
    dwell_duration_seconds: float
    dwell_duration_minutes: float
    ping_count: int
    avg_speed_mps: float
    max_speed_mps: float
    event_type: str                   # 'confirmed_store_visit' or 'catchment_presence'
    is_confirmed_visit: bool
    buffer_radius_meters: float = 500.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert event dataclass to dictionary."""
        return asdict(self)


@dataclass
class VisitDetectionSummary:
    """Statistical summary of store visit and proximity detection execution."""

    total_input_pings: int
    pings_in_catchments: int
    total_events_detected: int
    confirmed_store_visits: int
    catchment_presence_events: int
    unique_devices_detected: int
    stores_visited: Dict[str, int] = field(default_factory=dict)
    dwell_time_summary_minutes: Dict[str, float] = field(default_factory=dict)
    output_files: Dict[str, str] = field(default_factory=dict)
    validation_results: Dict[str, Any] = field(default_factory=dict)
    generated_at_utc: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> Dict[str, Any]:
        """Convert summary to dictionary."""
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        """Serialize summary to formatted JSON string."""
        return json.dumps(self.to_dict(), indent=indent)


def parse_utc_timestamp(ts: Any) -> datetime:
    """Parse varied timestamp representations into timezone-aware UTC datetime."""
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)

    ts_str = str(ts).strip()
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        # Try pandas timestamp parsing fallback
        import pandas as pd
        pdt = pd.to_datetime(ts, utc=True)
        return pdt.to_pydatetime()


def validate_input_pings(df: Any) -> Tuple[bool, List[str]]:
    """Validate mandatory fields, coordinates, and timestamp consistency for input telemetry."""
    errors: List[str] = []
    required = ["ping_id", "device_id", "timestamp_utc", "latitude", "longitude"]

    for col in required:
        if col not in df.columns:
            errors.append(f"Missing mandatory column: '{col}'")

    if errors:
        return False, errors

    # Check for empty dataframe
    if len(df) == 0:
        errors.append("Input telemetry dataframe is empty.")
        return False, errors

    # Check for null required values
    null_counts = df[required].isnull().sum().to_dict()
    for col, count in null_counts.items():
        if count > 0:
            errors.append(f"Column '{col}' has {count:,} null values.")

    # Check coordinate ranges
    lat_invalid = ((df["latitude"] < -90.0) | (df["latitude"] > 90.0)).sum()
    if lat_invalid > 0:
        errors.append(f"Found {lat_invalid} latitude values outside [-90.0, 90.0].")

    lon_invalid = ((df["longitude"] < -180.0) | (df["longitude"] > 180.0)).sum()
    if lon_invalid > 0:
        errors.append(f"Found {lon_invalid} longitude values outside [-180.0, 180.0].")

    is_valid = len(errors) == 0
    return is_valid, errors


def validate_visit_events(events: List[StoreVisitEvent]) -> Tuple[bool, List[str]]:
    """Validate generated visit events for duration, unique IDs, and timestamp monotonicity."""
    errors: List[str] = []
    seen_event_ids = set()

    for idx, evt in enumerate(events):
        # 1. Check event ID uniqueness
        if evt.event_id in seen_event_ids:
            errors.append(f"Duplicate event_id detected: '{evt.event_id}' at index {idx}.")
        else:
            seen_event_ids.add(evt.event_id)

        # 2. Check non-negative dwell duration
        if evt.dwell_duration_seconds < 0.0:
            errors.append(
                f"Negative dwell duration in event '{evt.event_id}': {evt.dwell_duration_seconds}s."
            )

        # 3. Check timestamps order
        entry_dt = parse_utc_timestamp(evt.entry_timestamp_utc)
        exit_dt = parse_utc_timestamp(evt.exit_timestamp_utc)
        if exit_dt < entry_dt:
            errors.append(
                f"Exit timestamp ({evt.exit_timestamp_utc}) precedes entry timestamp "
                f"({evt.entry_timestamp_utc}) in event '{evt.event_id}'."
            )

        # 4. Check ping count
        if evt.ping_count <= 0:
            errors.append(f"Event '{evt.event_id}' has non-positive ping_count: {evt.ping_count}.")

    is_valid = len(errors) == 0
    return is_valid, errors


class StoreVisitDetector:
    """Detects continuous store catchment presence and confirmed retail visit events."""

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        spatial_join_engine: Optional[SedonaSpatialJoinEngine] = None,
        min_dwell_minutes: float = DEFAULT_MIN_DWELL_MINUTES,
        max_dwell_minutes: float = DEFAULT_MAX_DWELL_MINUTES,
        speed_threshold_mps: float = DEFAULT_SPEED_THRESHOLD_MPS,
        max_session_gap_seconds: float = DEFAULT_MAX_SESSION_GAP_SECONDS,
        min_visit_pings: int = DEFAULT_MIN_VISIT_PINGS,
    ):
        """Initialize StoreVisitDetector with configurable dwell thresholds.

        Args:
            config: Base YAML config dictionary.
            spatial_join_engine: Optional SedonaSpatialJoinEngine instance.
            min_dwell_minutes: Threshold in minutes for confirmed store visit (default 5.0).
            max_dwell_minutes: Upper limit for dwell sanity checking (default 240.0).
            speed_threshold_mps: Maximum speed in m/s qualifying as stationary dwell (default 1.5).
            max_session_gap_seconds: Time gap in seconds breaking continuous stay (default 300).
            min_visit_pings: Minimum pings required to confirm physical visit (default 3).
        """
        self.config = config or load_yaml_config()
        self.spatial_join_engine = spatial_join_engine or SedonaSpatialJoinEngine(config=self.config)

        # Extract criteria from config if present
        dwell_cfg = self.config.get("dwell_time_criteria", {})
        self.min_dwell_minutes = float(dwell_cfg.get("min_dwell_minutes", min_dwell_minutes))
        self.max_dwell_minutes = float(dwell_cfg.get("max_dwell_minutes", max_dwell_minutes))
        self.max_session_gap_seconds = float(max_session_gap_seconds)
        self.speed_threshold_mps = float(speed_threshold_mps)
        self.min_visit_pings = int(min_visit_pings)

    def detect_continuous_events(
        self,
        joined_df: Any,
        max_gap_seconds: Optional[float] = None,
    ) -> List[StoreVisitEvent]:
        """Group spatially matched pings into continuous visit sessions per device and store.

        Contiguous readings with interval <= max_gap_seconds are aggregated into a single event.
        Events exceeding min_dwell_minutes with low speed are labeled 'confirmed_store_visit',
        while shorter or high-speed transitions are labeled 'catchment_presence'.

        Args:
            joined_df: Spatial join output DataFrame or GeoDataFrame.
            max_gap_seconds: Optional override for session timeout.

        Returns:
            List of StoreVisitEvent objects.
        """
        if len(joined_df) == 0:
            logger.warning("Spatial join returned 0 matched pings. No events detected.")
            return []

        gap_limit = max_gap_seconds if max_gap_seconds is not None else self.max_session_gap_seconds

        # Ensure timestamp column is parsed
        df = joined_df.copy()
        if not pd.api.types.is_datetime64_any_dtype(df["timestamp_utc"]):
            df["dt_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
        else:
            df["dt_utc"] = df["timestamp_utc"]

        # Ensure speed is numeric
        if "speed_mps" in df.columns:
            df["speed_mps"] = pd.to_numeric(df["speed_mps"], errors="coerce").fillna(0.0)
        else:
            df["speed_mps"] = 0.0

        # Sort chronologically by device, store, and timestamp
        df = df.sort_values(by=["device_id", "store_id", "dt_utc"]).reset_index(drop=True)

        events: List[StoreVisitEvent] = []

        # Sessionize per (device_id, store_id)
        for (dev_id, store_id), group in df.groupby(["device_id", "store_id"]):
            store_name = str(group["store_name"].iloc[0] if "store_name" in group.columns else store_id)
            store_cat = str(group["category"].iloc[0] if "category" in group.columns else "Retail")
            buffer_radius = float(
                group["buffer_radius_meters"].iloc[0] if "buffer_radius_meters" in group.columns else 500.0
            )

            current_session_pings: List[Dict[str, Any]] = []
            session_counter = 1

            for _, row in group.iterrows():
                row_dt = row["dt_utc"]

                if not current_session_pings:
                    current_session_pings.append(row.to_dict())
                else:
                    prev_dt = current_session_pings[-1]["dt_utc"]
                    delta_sec = (row_dt - prev_dt).total_seconds()

                    if delta_sec <= gap_limit:
                        current_session_pings.append(row.to_dict())
                    else:
                        # Session split: finalize current session
                        evt = self._build_event(
                            pings=current_session_pings,
                            dev_id=str(dev_id),
                            store_id=str(store_id),
                            store_name=store_name,
                            store_cat=store_cat,
                            buffer_radius=buffer_radius,
                            session_idx=session_counter,
                        )
                        events.append(evt)
                        session_counter += 1
                        current_session_pings = [row.to_dict()]

            # Finalize trailing session
            if current_session_pings:
                evt = self._build_event(
                    pings=current_session_pings,
                    dev_id=str(dev_id),
                    store_id=str(store_id),
                    store_name=store_name,
                    store_cat=store_cat,
                    buffer_radius=buffer_radius,
                    session_idx=session_counter,
                )
                events.append(evt)

        logger.info(
            f"Event detection complete: {len(events)} total events generated from {len(joined_df)} pings."
        )
        return events

    def _build_event(
        self,
        pings: List[Dict[str, Any]],
        dev_id: str,
        store_id: str,
        store_name: str,
        store_cat: str,
        buffer_radius: float,
        session_idx: int,
    ) -> StoreVisitEvent:
        """Construct a validated StoreVisitEvent from a continuous sequence of pings."""
        first_ping = pings[0]
        last_ping = pings[-1]

        entry_dt = first_ping["dt_utc"]
        exit_dt = last_ping["dt_utc"]

        dwell_sec = max(0.0, (exit_dt - entry_dt).total_seconds())
        dwell_min = round(dwell_sec / 60.0, 2)

        speeds = [float(p.get("speed_mps", 0.0)) for p in pings]
        avg_speed = round(sum(speeds) / len(speeds), 2) if speeds else 0.0
        max_speed = round(max(speeds), 2) if speeds else 0.0

        # Classification: distinguish catchment presence vs confirmed physical store visit
        is_confirmed = (
            dwell_min >= self.min_dwell_minutes
            and avg_speed <= self.speed_threshold_mps
            and len(pings) >= self.min_visit_pings
        )
        event_type = "confirmed_store_visit" if is_confirmed else "catchment_presence"

        # Timestamp strings
        entry_iso = entry_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        exit_iso = exit_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Deterministic unique event ID
        ts_clean = entry_dt.strftime("%Y%m%d%H%M%S")
        event_id = f"EVT_{dev_id}_{store_id}_{ts_clean}_{session_idx:02d}"
        trajectory_id = f"TRJ_{dev_id}"

        return StoreVisitEvent(
            event_id=event_id,
            device_id=dev_id,
            trajectory_id=trajectory_id,
            store_id=store_id,
            store_name=store_name,
            store_category=store_cat,
            entry_timestamp_utc=entry_iso,
            exit_timestamp_utc=exit_iso,
            dwell_duration_seconds=round(dwell_sec, 2),
            dwell_duration_minutes=dwell_min,
            ping_count=len(pings),
            avg_speed_mps=avg_speed,
            max_speed_mps=max_speed,
            event_type=event_type,
            is_confirmed_visit=is_confirmed,
            buffer_radius_meters=buffer_radius,
        )

    def events_to_dataframe(self, events: List[StoreVisitEvent]) -> Any:
        """Convert list of StoreVisitEvent objects to pandas DataFrame."""
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas is required to construct events dataframe.")

        if not events:
            # Return empty schema-aligned DataFrame
            return pd.DataFrame(columns=[
                "event_id", "device_id", "trajectory_id", "store_id", "store_name",
                "store_category", "entry_timestamp_utc", "exit_timestamp_utc",
                "dwell_duration_seconds", "dwell_duration_minutes", "ping_count",
                "avg_speed_mps", "max_speed_mps", "event_type", "is_confirmed_visit",
                "buffer_radius_meters",
            ])

        records = [e.to_dict() for e in events]
        df = pd.DataFrame(records)
        return df

    def save_outputs(
        self,
        events_df: Any,
        summary: VisitDetectionSummary,
        output_dir: Optional[Union[str, Path]] = None,
    ) -> Dict[str, str]:
        """Persist detected events to Parquet and CSV, and save summary JSON."""
        if output_dir is None:
            out_path = get_project_root() / "data" / "processed"
        else:
            out_path = Path(output_dir)

        out_path.mkdir(parents=True, exist_ok=True)

        parquet_path = out_path / "store_catchment_events.parquet"
        csv_path = out_path / "store_catchment_events.csv"
        summary_path = out_path / "store_visit_detection_summary.json"

        # 1. Save Parquet
        logger.info(f"Writing visit events to Parquet: {parquet_path}")
        events_df.to_parquet(str(parquet_path), index=False)

        # 2. Save CSV
        logger.info(f"Writing visit events to CSV: {csv_path}")
        events_df.to_csv(str(csv_path), index=False)

        # 3. Update summary file mappings
        written_files = {
            "events_parquet": str(parquet_path),
            "events_csv": str(csv_path),
            "summary_json": str(summary_path),
        }
        summary.output_files = written_files

        # 4. Save JSON summary
        logger.info(f"Writing visit detection summary to JSON: {summary_path}")
        with open(summary_path, "w", encoding="utf-8") as f:
            f.write(summary.to_json(indent=2))

        return written_files

    def run_detection_pipeline(
        self,
        pings_data: Optional[Union[str, Path, Any]] = None,
        catchments_data: Optional[Union[str, Path, Any]] = None,
        output_dir: Optional[Union[str, Path]] = None,
    ) -> VisitDetectionSummary:
        """Execute end-to-end Day 6 visit detection pipeline.

        1. Ingests cleaned GPS telemetry and validates schema.
        2. Ingests 500m store catchment polygons.
        3. Executes point-in-polygon spatial join.
        4. Sessionizes continuous stays and calculates dwell durations.
        5. Distinguishes catchment presence from confirmed retail visits.
        6. Validates event integrity and persists Parquet, CSV, and JSON deliverables.

        Args:
            pings_data: Optional path to GPS data (defaults to data/processed/cleaned_gps_data.csv).
            catchments_data: Optional path to catchments GeoJSON (defaults to Day 5 output).
            output_dir: Destination folder (defaults to data/processed/).

        Returns:
            VisitDetectionSummary dataclass instance.
        """
        # Resolve pings path
        if pings_data is None:
            pings_path = get_project_root() / "data" / "processed" / "cleaned_gps_data.csv"
        else:
            pings_path = Path(pings_data)

        if not Path(pings_path).exists() and not isinstance(pings_data, (pd.DataFrame, gpd.GeoDataFrame)):
            raise FileNotFoundError(f"Input GPS dataset not found at: {pings_path}")

        # 1. Load and validate input pings
        if isinstance(pings_path, (str, Path)):
            if Path(pings_path).suffix == ".parquet":
                raw_pings_df = pd.read_parquet(str(pings_path))
            else:
                raw_pings_df = pd.read_csv(str(pings_path))
        else:
            raw_pings_df = pings_path

        total_input_pings = len(raw_pings_df)
        pings_valid, pings_errs = validate_input_pings(raw_pings_df)
        if not pings_valid:
            raise ValueError(f"Input telemetry validation failed: {pings_errs}")

        # 2. Execute spatial join
        joined_df = self.spatial_join_engine.join_pings_with_catchments(
            pings_data=raw_pings_df,
            catchments_data=catchments_data,
        )
        pings_in_catchments = len(joined_df)

        # 3. Detect continuous events
        events = self.detect_continuous_events(joined_df)

        # 4. Validate output events
        events_valid, events_errs = validate_visit_events(events)
        if not events_valid:
            raise ValueError(f"Visit events validation failed: {events_errs}")

        # 5. Calculate statistical aggregates
        confirmed_count = sum(1 for e in events if e.is_confirmed_visit)
        presence_count = len(events) - confirmed_count
        unique_devices = len({e.device_id for e in events})

        store_visit_counts: Dict[str, int] = {}
        for e in events:
            store_visit_counts[e.store_id] = store_visit_counts.get(e.store_id, 0) + 1

        dwell_minutes_list = [e.dwell_duration_minutes for e in events]
        dwell_stats: Dict[str, float] = {}
        if dwell_minutes_list:
            dwell_stats = {
                "min_minutes": round(min(dwell_minutes_list), 2),
                "max_minutes": round(max(dwell_minutes_list), 2),
                "mean_minutes": round(sum(dwell_minutes_list) / len(dwell_minutes_list), 2),
            }

        summary = VisitDetectionSummary(
            total_input_pings=total_input_pings,
            pings_in_catchments=pings_in_catchments,
            total_events_detected=len(events),
            confirmed_store_visits=confirmed_count,
            catchment_presence_events=presence_count,
            unique_devices_detected=unique_devices,
            stores_visited=store_visit_counts,
            dwell_time_summary_minutes=dwell_stats,
            validation_results={
                "input_pings_valid": pings_valid,
                "visit_events_valid": events_valid,
                "input_errors": pings_errs,
                "event_errors": events_errs,
            },
        )

        # 6. Save deliverables
        events_df = self.events_to_dataframe(events)
        self.save_outputs(events_df=events_df, summary=summary, output_dir=output_dir)

        logger.info(
            f"Day 6 pipeline complete: {len(events)} events detected across {unique_devices} devices. "
            f"({confirmed_count} confirmed visits, {presence_count} catchment transits)."
        )
        return summary


if __name__ == "__main__":
    detector = StoreVisitDetector()
    summary = detector.run_detection_pipeline()
    print("\n" + "=" * 70)
    print(" GeoPulse Day 6: Store Visit Detection & Spatial Join Summary")
    print("=" * 70)
    print(summary.to_json(indent=2))
