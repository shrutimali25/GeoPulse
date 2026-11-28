"""Mobility Metrics Calculator & Store Analytics for GeoPulse.

Computes retail visit metrics from matched spatial pings and visit events:
- Contiguous dwell duration per device per store
- Store-level visit counts, device reach, and dwell statistics
- Device-level mobility profiling, store diversity, and dwell times
- Time-based hourly visit distribution and peak hour identification
- Validation of event data integrity and automated analytics pipeline
"""

from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = Any  # type: ignore

from src.common.config_loader import get_project_root, load_yaml_config
from src.common.logger import get_logger

logger = get_logger(__name__)

# Mandatory columns for visit events DataFrame
REQUIRED_EVENT_COLUMNS = [
    "event_id",
    "device_id",
    "store_id",
    "entry_timestamp_utc",
    "exit_timestamp_utc",
    "dwell_duration_minutes",
    "is_confirmed_visit",
]


def validate_visit_events_dataframe(df: Any) -> Tuple[bool, List[str]]:
    """Validate DataFrame of store visit events against schema and business integrity rules.

    Checks:
    - Presence of mandatory columns
    - Non-null values in essential identifiers
    - Non-negative dwell duration
    - Chronological ordering of entry and exit timestamps
    - Uniqueness of event IDs
    - Graceful empty dataset handling

    Args:
        df: DataFrame containing visit event records.

    Returns:
        Tuple of (is_valid: bool, errors: List[str]).
    """
    errors: List[str] = []

    if df is None or not hasattr(df, "columns"):
        return False, ["Input is not a valid DataFrame."]

    if len(df) == 0:
        return True, []

    # 1. Mandatory columns check
    missing_cols = [col for col in REQUIRED_EVENT_COLUMNS if col not in df.columns]
    if missing_cols:
        errors.append(f"Missing mandatory columns in visit events: {', '.join(missing_cols)}.")
        return False, errors

    # 2. Null value checks on essential fields
    for col in ["event_id", "device_id", "store_id", "entry_timestamp_utc", "exit_timestamp_utc"]:
        null_count = int(df[col].isnull().sum())
        if null_count > 0:
            errors.append(f"Column '{col}' contains {null_count} null/missing value(s).")

    # 3. Duplicate event IDs
    dup_count = int(df["event_id"].duplicated().sum())
    if dup_count > 0:
        dup_ids = list(df.loc[df["event_id"].duplicated(), "event_id"].unique()[:5])
        errors.append(f"Found {dup_count} duplicate event_id(s) (e.g. {dup_ids}).")

    # 4. Negative dwell durations
    dwell_numeric = pd.to_numeric(df["dwell_duration_minutes"], errors="coerce")
    neg_dwell = int((dwell_numeric < 0).sum())
    if neg_dwell > 0:
        errors.append(f"Found {neg_dwell} record(s) with negative dwell_duration_minutes.")

    # 5. Timestamp chronological consistency
    try:
        entry_ts = pd.to_datetime(df["entry_timestamp_utc"], utc=True, errors="coerce")
        exit_ts = pd.to_datetime(df["exit_timestamp_utc"], utc=True, errors="coerce")
        invalid_ts_order = int((exit_ts < entry_ts).sum())
        if invalid_ts_order > 0:
            errors.append(
                f"Found {invalid_ts_order} record(s) where exit_timestamp is earlier than entry_timestamp."
            )
    except Exception as ex:
        errors.append(f"Failed to parse timestamps for order validation: {ex}")

    return len(errors) == 0, errors


def compute_store_visit_analytics(events_df: Any) -> pd.DataFrame:
    """Aggregate store-level visit counts, device reach, dwell metrics, and categories.

    Produces:
    - total_detected_visits
    - confirmed_visits
    - catchment_presence_events
    - unique_devices
    - total_dwell_minutes
    - avg_dwell_minutes
    - min_dwell_minutes
    - max_dwell_minutes

    Args:
        events_df: DataFrame of detected visit events.

    Returns:
        DataFrame with store-level mobility analytics sorted by visit count descending.
    """
    cols = [
        "store_id",
        "store_name",
        "store_category",
        "total_detected_visits",
        "confirmed_visits",
        "catchment_presence_events",
        "unique_devices",
        "total_dwell_minutes",
        "avg_dwell_minutes",
        "min_dwell_minutes",
        "max_dwell_minutes",
    ]
    if events_df is None or len(events_df) == 0:
        return pd.DataFrame(columns=cols)

    is_valid, errors = validate_visit_events_dataframe(events_df)
    if not is_valid:
        raise ValueError(f"Invalid events DataFrame: {'; '.join(errors)}")

    df = events_df.copy()
    if "store_name" not in df.columns:
        df["store_name"] = df["store_id"]
    if "store_category" not in df.columns:
        df["store_category"] = "Retail"
    if "is_confirmed_visit" in df.columns:
        df["is_confirmed_visit"] = df["is_confirmed_visit"].astype(bool)
    else:
        df["is_confirmed_visit"] = True

    records = []
    for store_id, group in df.groupby("store_id", as_index=False):
        store_name = str(group["store_name"].iloc[0])
        store_category = str(group["store_category"].iloc[0])
        total_visits = len(group)
        confirmed_visits = int(group["is_confirmed_visit"].sum())
        presence_events = total_visits - confirmed_visits
        unique_devices = int(group["device_id"].nunique())

        dwells = pd.to_numeric(group["dwell_duration_minutes"], errors="coerce").dropna()
        total_dwell = round(float(dwells.sum()), 2) if len(dwells) > 0 else 0.0
        avg_dwell = round(float(dwells.mean()), 2) if len(dwells) > 0 else 0.0
        min_dwell = round(float(dwells.min()), 2) if len(dwells) > 0 else 0.0
        max_dwell = round(float(dwells.max()), 2) if len(dwells) > 0 else 0.0

        records.append({
            "store_id": str(store_id),
            "store_name": store_name,
            "store_category": store_category,
            "total_detected_visits": total_visits,
            "confirmed_visits": confirmed_visits,
            "catchment_presence_events": presence_events,
            "unique_devices": unique_devices,
            "total_dwell_minutes": total_dwell,
            "avg_dwell_minutes": avg_dwell,
            "min_dwell_minutes": min_dwell,
            "max_dwell_minutes": max_dwell,
        })

    result_df = pd.DataFrame(records)
    if len(result_df) > 0:
        result_df = result_df.sort_values(
            by=["total_detected_visits", "avg_dwell_minutes", "store_id"],
            ascending=[False, False, True],
        ).reset_index(drop=True)
    return result_df[cols]


def compute_device_mobility_analytics(events_df: Any) -> pd.DataFrame:
    """Compute per-device visit activity, store diversity, dwell times, and temporal bounds.

    Produces:
    - total_detected_visits
    - confirmed_visits
    - catchment_presence_events
    - total_stores_visited
    - unique_stores_visited
    - total_dwell_minutes
    - avg_dwell_minutes
    - min_dwell_minutes
    - max_dwell_minutes
    - first_visit_utc
    - last_visit_utc

    Args:
        events_df: DataFrame of detected visit events.

    Returns:
        DataFrame with device-level mobility and dwell analytics.
    """
    cols = [
        "device_id",
        "total_detected_visits",
        "confirmed_visits",
        "catchment_presence_events",
        "total_stores_visited",
        "unique_stores_visited",
        "total_dwell_minutes",
        "avg_dwell_minutes",
        "min_dwell_minutes",
        "max_dwell_minutes",
        "first_visit_utc",
        "last_visit_utc",
    ]
    if events_df is None or len(events_df) == 0:
        return pd.DataFrame(columns=cols)

    is_valid, errors = validate_visit_events_dataframe(events_df)
    if not is_valid:
        raise ValueError(f"Invalid events DataFrame: {'; '.join(errors)}")

    df = events_df.copy()
    if "is_confirmed_visit" in df.columns:
        df["is_confirmed_visit"] = df["is_confirmed_visit"].astype(bool)
    else:
        df["is_confirmed_visit"] = True

    records = []
    for device_id, group in df.groupby("device_id", as_index=False):
        total_visits = len(group)
        confirmed_visits = int(group["is_confirmed_visit"].sum())
        presence_events = total_visits - confirmed_visits
        total_stores_visited = len(group)
        unique_stores_visited = int(group["store_id"].nunique())

        dwells = pd.to_numeric(group["dwell_duration_minutes"], errors="coerce").dropna()
        total_dwell = round(float(dwells.sum()), 2) if len(dwells) > 0 else 0.0
        avg_dwell = round(float(dwells.mean()), 2) if len(dwells) > 0 else 0.0
        min_dwell = round(float(dwells.min()), 2) if len(dwells) > 0 else 0.0
        max_dwell = round(float(dwells.max()), 2) if len(dwells) > 0 else 0.0

        first_visit = str(group["entry_timestamp_utc"].min())
        last_visit = str(group["exit_timestamp_utc"].max())

        records.append({
            "device_id": str(device_id),
            "total_detected_visits": total_visits,
            "confirmed_visits": confirmed_visits,
            "catchment_presence_events": presence_events,
            "total_stores_visited": total_stores_visited,
            "unique_stores_visited": unique_stores_visited,
            "total_dwell_minutes": total_dwell,
            "avg_dwell_minutes": avg_dwell,
            "min_dwell_minutes": min_dwell,
            "max_dwell_minutes": max_dwell,
            "first_visit_utc": first_visit,
            "last_visit_utc": last_visit,
        })

    result_df = pd.DataFrame(records)
    if len(result_df) > 0:
        result_df = result_df.sort_values(
            by=["total_detected_visits", "unique_stores_visited", "device_id"],
            ascending=[False, False, True],
        ).reset_index(drop=True)
    return result_df[cols]


def compute_hourly_visit_analytics(events_df: Any) -> pd.DataFrame:
    """Calculate hourly visit distributions, peak hour flags, and store-level hourly profiles.

    Produces:
    - hour_of_day_utc (0-23)
    - store_id
    - store_name
    - store_category
    - visit_count
    - confirmed_visit_count
    - unique_devices
    - avg_dwell_minutes
    - is_peak_hour

    Args:
        events_df: DataFrame of detected visit events with UTC timestamps.

    Returns:
        DataFrame with store-level and hour-level visit analytics.
    """
    cols = [
        "hour_of_day_utc",
        "store_id",
        "store_name",
        "store_category",
        "visit_count",
        "confirmed_visit_count",
        "unique_devices",
        "avg_dwell_minutes",
        "is_peak_hour",
    ]
    if events_df is None or len(events_df) == 0:
        return pd.DataFrame(columns=cols)

    is_valid, errors = validate_visit_events_dataframe(events_df)
    if not is_valid:
        raise ValueError(f"Invalid events DataFrame: {'; '.join(errors)}")

    df = events_df.copy()
    if "store_name" not in df.columns:
        df["store_name"] = df["store_id"]
    if "store_category" not in df.columns:
        df["store_category"] = "Retail"
    if "is_confirmed_visit" in df.columns:
        df["is_confirmed_visit"] = df["is_confirmed_visit"].astype(bool)
    else:
        df["is_confirmed_visit"] = True

    dt_series = pd.to_datetime(df["entry_timestamp_utc"], utc=True)
    df["hour_of_day_utc"] = dt_series.dt.hour

    # Identify overall peak visiting hours across all stores
    hourly_overall_counts = df.groupby("hour_of_day_utc")["event_id"].count()
    max_count = hourly_overall_counts.max() if len(hourly_overall_counts) > 0 else 0
    peak_hours = (
        set(hourly_overall_counts[hourly_overall_counts == max_count].index)
        if max_count > 0
        else set()
    )

    records = []
    for (hour, store_id), group in df.groupby(["hour_of_day_utc", "store_id"], as_index=False):
        store_name = str(group["store_name"].iloc[0])
        store_category = str(group["store_category"].iloc[0])
        visit_count = len(group)
        confirmed_count = int(group["is_confirmed_visit"].sum())
        unique_devices = int(group["device_id"].nunique())

        dwells = pd.to_numeric(group["dwell_duration_minutes"], errors="coerce").dropna()
        avg_dwell = round(float(dwells.mean()), 2) if len(dwells) > 0 else 0.0

        records.append({
            "hour_of_day_utc": int(hour),
            "store_id": str(store_id),
            "store_name": store_name,
            "store_category": store_category,
            "visit_count": visit_count,
            "confirmed_visit_count": confirmed_count,
            "unique_devices": unique_devices,
            "avg_dwell_minutes": avg_dwell,
            "is_peak_hour": bool(int(hour) in peak_hours),
        })

    result_df = pd.DataFrame(records)
    if len(result_df) > 0:
        result_df = result_df.sort_values(
            by=["hour_of_day_utc", "visit_count", "store_id"],
            ascending=[True, False, True],
        ).reset_index(drop=True)
    return result_df[cols]


def compute_hourly_distribution(events_df: Any) -> pd.DataFrame:
    """Compute overall hour-of-day visit distribution aggregated across all stores.

    Args:
        events_df: DataFrame of detected visit events.

    Returns:
        DataFrame with columns: hour_of_day_utc, total_visits, confirmed_visits,
        unique_devices, avg_dwell_minutes, is_peak_hour.
    """
    cols = [
        "hour_of_day_utc",
        "total_visits",
        "confirmed_visits",
        "unique_devices",
        "avg_dwell_minutes",
        "is_peak_hour",
    ]
    if events_df is None or len(events_df) == 0:
        return pd.DataFrame(columns=cols)

    df = events_df.copy()
    if "is_confirmed_visit" in df.columns:
        df["is_confirmed_visit"] = df["is_confirmed_visit"].astype(bool)
    else:
        df["is_confirmed_visit"] = True

    dt_series = pd.to_datetime(df["entry_timestamp_utc"], utc=True)
    df["hour_of_day_utc"] = dt_series.dt.hour

    hourly_counts = df.groupby("hour_of_day_utc")["event_id"].count()
    max_count = hourly_counts.max() if len(hourly_counts) > 0 else 0
    peak_hours = set(hourly_counts[hourly_counts == max_count].index) if max_count > 0 else set()

    records = []
    for hour, group in df.groupby("hour_of_day_utc", as_index=False):
        total_visits = len(group)
        confirmed_visits = int(group["is_confirmed_visit"].sum())
        unique_devices = int(group["device_id"].nunique())
        dwells = pd.to_numeric(group["dwell_duration_minutes"], errors="coerce").dropna()
        avg_dwell = round(float(dwells.mean()), 2) if len(dwells) > 0 else 0.0

        records.append({
            "hour_of_day_utc": int(hour),
            "total_visits": total_visits,
            "confirmed_visits": confirmed_visits,
            "unique_devices": unique_devices,
            "avg_dwell_minutes": avg_dwell,
            "is_peak_hour": bool(int(hour) in peak_hours),
        })
    res = pd.DataFrame(records)
    if len(res) > 0:
        res = res.sort_values(by="hour_of_day_utc").reset_index(drop=True)
    return res[cols]


def generate_analytics_summary(
    events_df: Any,
    store_df: Any,
    device_df: Any,
    hourly_df: Any,
    output_files: Optional[Dict[str, str]] = None,
    source_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Generate comprehensive JSON-serializable mobility analytics summary.

    Args:
        events_df: Input events DataFrame.
        store_df: Store-level analytics DataFrame.
        device_df: Device-level analytics DataFrame.
        hourly_df: Hourly visit analytics DataFrame.
        output_files: Dictionary mapping deliverable names to saved file paths.
        source_path: Source events file path or identifier.

    Returns:
        Structured dictionary matching GeoPulse analytics schema.
    """
    now_utc = datetime.now(timezone.utc).isoformat()

    if events_df is None or len(events_df) == 0:
        return {
            "analytics_metadata": {
                "generated_at_utc": now_utc,
                "source_events_file": source_path or "",
                "total_input_events": 0,
                "validation_status": "EMPTY_DATASET",
            },
            "overall_summary": {
                "total_detected_visits": 0,
                "total_confirmed_visits": 0,
                "total_catchment_presence_events": 0,
                "unique_devices": 0,
                "unique_stores": 0,
                "dwell_time_summary_minutes": {
                    "total_minutes": 0.0,
                    "mean_minutes": 0.0,
                    "min_minutes": 0.0,
                    "max_minutes": 0.0,
                },
            },
            "hourly_insights": {
                "peak_visiting_hours_utc": [],
                "peak_hour_visit_count": 0,
                "hourly_distribution_utc": {},
            },
            "store_rankings": {
                "by_total_visits": [],
                "by_avg_dwell_minutes": [],
            },
            "device_mobility_summary": {
                "avg_stores_per_device": 0.0,
                "max_stores_by_single_device": 0,
                "most_active_device": None,
                "avg_dwell_per_device_minutes": 0.0,
            },
            "output_files": output_files or {},
        }

    dwells = pd.to_numeric(events_df["dwell_duration_minutes"], errors="coerce").dropna()
    total_dwell = round(float(dwells.sum()), 2) if len(dwells) > 0 else 0.0
    mean_dwell = round(float(dwells.mean()), 2) if len(dwells) > 0 else 0.0
    min_dwell = round(float(dwells.min()), 2) if len(dwells) > 0 else 0.0
    max_dwell = round(float(dwells.max()), 2) if len(dwells) > 0 else 0.0

    total_detected = len(events_df)
    confirmed_visits = (
        int(events_df["is_confirmed_visit"].astype(bool).sum())
        if "is_confirmed_visit" in events_df.columns
        else total_detected
    )
    catchment_presence = total_detected - confirmed_visits

    # Hourly distribution & peak hours
    dt_series = pd.to_datetime(events_df["entry_timestamp_utc"], utc=True)
    hours = dt_series.dt.hour
    hourly_counts = hours.value_counts().to_dict()
    hourly_distribution = {str(k): int(v) for k, v in sorted(hourly_counts.items())}
    max_hourly_val = max(hourly_counts.values()) if hourly_counts else 0
    peak_hours = (
        sorted([int(k) for k, v in hourly_counts.items() if v == max_hourly_val])
        if max_hourly_val > 0
        else []
    )

    # Store rankings
    by_visits: List[Dict[str, Any]] = []
    by_dwell: List[Dict[str, Any]] = []
    if store_df is not None and len(store_df) > 0:
        sorted_by_v = store_df.sort_values(
            by=["total_detected_visits", "confirmed_visits"], ascending=[False, False]
        )
        by_visits = [
            {
                "store_id": str(r["store_id"]),
                "store_name": str(r["store_name"]),
                "total_visits": int(r["total_detected_visits"]),
                "confirmed_visits": int(r["confirmed_visits"]),
            }
            for _, r in sorted_by_v.iterrows()
        ]
        sorted_by_d = store_df.sort_values(by="avg_dwell_minutes", ascending=False)
        by_dwell = [
            {
                "store_id": str(r["store_id"]),
                "store_name": str(r["store_name"]),
                "avg_dwell_minutes": float(r["avg_dwell_minutes"]),
            }
            for _, r in sorted_by_d.iterrows()
        ]

    # Device mobility
    avg_stores_per_dev = (
        round(float(device_df["unique_stores_visited"].mean()), 2)
        if device_df is not None and len(device_df) > 0
        else 0.0
    )
    max_stores_dev = (
        int(device_df["unique_stores_visited"].max())
        if device_df is not None and len(device_df) > 0
        else 0
    )
    most_active_dev = (
        str(device_df.iloc[0]["device_id"])
        if device_df is not None and len(device_df) > 0
        else None
    )
    avg_dwell_dev = (
        round(float(device_df["total_dwell_minutes"].mean()), 2)
        if device_df is not None and len(device_df) > 0
        else 0.0
    )

    return {
        "analytics_metadata": {
            "generated_at_utc": now_utc,
            "source_events_file": source_path or "",
            "total_input_events": total_detected,
            "validation_status": "PASSED",
        },
        "overall_summary": {
            "total_detected_visits": total_detected,
            "total_confirmed_visits": confirmed_visits,
            "total_catchment_presence_events": catchment_presence,
            "unique_devices": int(events_df["device_id"].nunique()),
            "unique_stores": int(events_df["store_id"].nunique()),
            "dwell_time_summary_minutes": {
                "total_minutes": total_dwell,
                "mean_minutes": mean_dwell,
                "min_minutes": min_dwell,
                "max_minutes": max_dwell,
            },
        },
        "hourly_insights": {
            "peak_visiting_hours_utc": peak_hours,
            "peak_hour_visit_count": max_hourly_val,
            "hourly_distribution_utc": hourly_distribution,
        },
        "store_rankings": {
            "by_total_visits": by_visits,
            "by_avg_dwell_minutes": by_dwell,
        },
        "device_mobility_summary": {
            "avg_stores_per_device": avg_stores_per_dev,
            "max_stores_by_single_device": max_stores_dev,
            "most_active_device": most_active_dev,
            "avg_dwell_per_device_minutes": avg_dwell_dev,
        },
        "output_files": output_files or {},
    }


def run_analytics_pipeline(
    events_path: Optional[Union[str, Path, Any]] = None,
    output_dir: Optional[Union[str, Path]] = None,
) -> Dict[str, Any]:
    """Execute end-to-end Day 7 mobility analytics pipeline.

    1. Ingests store catchment events from CSV or Parquet.
    2. Validates events schema and data integrity.
    3. Computes store-level, device-level, and hourly mobility analytics.
    4. Persists CSV deliverables and summary JSON to output_dir.

    Args:
        events_path: Path to store_catchment_events.csv / .parquet, or DataFrame.
        output_dir: Destination directory (defaults to data/processed/analytics).

    Returns:
        Dictionary containing analytics summary and output file paths.
    """
    root = get_project_root()

    # Resolve input source
    if events_path is None:
        csv_default = root / "data" / "processed" / "store_catchment_events.csv"
        parquet_default = root / "data" / "processed" / "store_catchment_events.parquet"
        if csv_default.exists():
            resolved_source: Any = csv_default
        elif parquet_default.exists():
            resolved_source = parquet_default
        else:
            raise FileNotFoundError(f"Default events dataset not found: {csv_default}")
    else:
        resolved_source = events_path

    if hasattr(resolved_source, "columns"):
        df_events = resolved_source.copy()
        source_label = "DataFrame"
    else:
        p = Path(resolved_source)
        if not p.exists():
            raise FileNotFoundError(f"Events dataset not found: {p}")
        source_label = str(p)
        if p.suffix == ".parquet":
            df_events = pd.read_parquet(p)
        else:
            df_events = pd.read_csv(p)

    # Resolve output directory
    target_dir = Path(output_dir) if output_dir else root / "data" / "processed" / "analytics"
    target_dir.mkdir(parents=True, exist_ok=True)

    # 1. Validation
    is_valid, errors = validate_visit_events_dataframe(df_events)
    if not is_valid:
        raise ValueError(f"Events data failed validation: {'; '.join(errors)}")

    # 2. Compute analytics
    store_df = compute_store_visit_analytics(df_events)
    device_df = compute_device_mobility_analytics(df_events)
    hourly_df = compute_hourly_visit_analytics(df_events)

    # 3. Define output paths
    store_csv_path = target_dir / "store_visit_analytics.csv"
    device_csv_path = target_dir / "device_mobility_analytics.csv"
    hourly_csv_path = target_dir / "hourly_visit_analytics.csv"
    summary_json_path = target_dir / "mobility_analytics_summary.json"

    # 4. Save CSV deliverables
    store_df.to_csv(store_csv_path, index=False)
    device_df.to_csv(device_csv_path, index=False)
    hourly_df.to_csv(hourly_csv_path, index=False)

    output_files = {
        "store_visit_analytics_csv": str(store_csv_path),
        "device_mobility_analytics_csv": str(device_csv_path),
        "hourly_visit_analytics_csv": str(hourly_csv_path),
        "mobility_analytics_summary_json": str(summary_json_path),
    }

    # 5. Generate and save summary JSON
    summary = generate_analytics_summary(
        events_df=df_events,
        store_df=store_df,
        device_df=device_df,
        hourly_df=hourly_df,
        output_files=output_files,
        source_path=source_label,
    )

    with open(summary_json_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    logger.info(
        f"Day 7 Mobility Analytics successfully generated in {target_dir}. "
        f"Stores: {len(store_df)}, Devices: {len(device_df)}, Hourly rows: {len(hourly_df)}."
    )
    return summary


class MobilityMetricsCalculator:
    """Calculates visit intervals, dwell durations, and spatial mobility analytics."""

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
        events = detector.detect_continuous_events(
            matched_pings_df, max_gap_seconds=self.max_gap_seconds
        )
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

    def validate_events(self, df: Any) -> Tuple[bool, List[str]]:
        """Validate visit events DataFrame."""
        return validate_visit_events_dataframe(df)

    def compute_store_analytics(self, df: Any) -> pd.DataFrame:
        """Compute store-level visit metrics and dwell times."""
        return compute_store_visit_analytics(df)

    def compute_device_analytics(self, df: Any) -> pd.DataFrame:
        """Compute per-device mobility and dwell profile."""
        return compute_device_mobility_analytics(df)

    def compute_hourly_analytics(self, df: Any) -> pd.DataFrame:
        """Compute hourly visit profile and peak hour flags."""
        return compute_hourly_visit_analytics(df)

    def compute_hourly_distribution(self, df: Any) -> pd.DataFrame:
        """Compute aggregate hourly visit distribution."""
        return compute_hourly_distribution(df)

    def generate_summary(
        self,
        events_df: Any,
        store_df: Any,
        device_df: Any,
        hourly_df: Any,
        output_files: Optional[Dict[str, str]] = None,
        source_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Generate structured mobility summary dictionary."""
        return generate_analytics_summary(
            events_df=events_df,
            store_df=store_df,
            device_df=device_df,
            hourly_df=hourly_df,
            output_files=output_files,
            source_path=source_path,
        )

    def run_analytics(
        self,
        events_input: Optional[Union[str, Path, Any]] = None,
        output_dir: Optional[Union[str, Path]] = None,
    ) -> Dict[str, Any]:
        """Execute complete mobility analytics pipeline."""
        return run_analytics_pipeline(events_path=events_input, output_dir=output_dir)


# Alias for intuitive discovery
StoreMobilityAnalytics = MobilityMetricsCalculator
