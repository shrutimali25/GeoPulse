"""Unit tests for store proximity, spatial joins, and visit event detection.

Verifies:
- Point-in-polygon spatial join between GPS telemetry and 500m store catchments
- Continuous stay sessionization and dwell duration computation
- Clear distinction between catchment presence and confirmed physical store visits
- Handling of single-ping pass-throughs and multi-visit temporal splits
- Input validation (missing columns, invalid coordinates, nulls)
- Output validation (non-negative dwell, monotonic timestamps, unique event IDs)
- Persistence of Parquet, CSV, and JSON audit deliverables
"""

from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import pytest
import pandas as pd

from src.common.config_loader import get_project_root
from src.member1_data_eng.spatial_processing.mobility_metrics import MobilityMetricsCalculator
from src.member1_data_eng.spatial_processing.sedona_spatial_joins import (
    GEOPANDAS_AVAILABLE,
    SedonaSpatialJoinEngine,
)
from src.member1_data_eng.spatial_processing.visit_detector import (
    StoreVisitDetector,
    StoreVisitEvent,
    VisitDetectionSummary,
    parse_utc_timestamp,
    validate_input_pings,
    validate_visit_events,
)


@pytest.mark.skipif(not GEOPANDAS_AVAILABLE, reason="geopandas is not installed in the active environment")
def test_spatial_join_with_500m_catchments():
    """Verify point-in-polygon spatial join between cleaned GPS data and store catchments."""
    cleaned_csv = get_project_root() / "data" / "processed" / "cleaned_gps_data.csv"
    catchments_geojson = get_project_root() / "data" / "spatial" / "store_catchments_500m.geojson"

    assert cleaned_csv.exists(), f"Cleaned GPS dataset missing: {cleaned_csv}"
    assert catchments_geojson.exists(), f"Catchments GeoJSON missing: {catchments_geojson}"

    engine = SedonaSpatialJoinEngine()
    joined_df = engine.join_pings_with_catchments(
        pings_data=cleaned_csv,
        catchments_data=catchments_geojson,
    )

    # 1. 258 of 300 pings fall inside 500m store catchments
    assert len(joined_df) == 258
    assert "store_id" in joined_df.columns
    assert "store_name" in joined_df.columns
    assert "buffer_radius_meters" in joined_df.columns

    # 2. All 5 stores have matched pings
    matched_stores = set(joined_df["store_id"].unique())
    assert len(matched_stores) == 5
    assert "RET_CAFE_STARBUCKS_002" in matched_stores
    assert "RET_ELEC_CROMA_003" in matched_stores


@pytest.mark.skipif(not GEOPANDAS_AVAILABLE, reason="geopandas is not installed in the active environment")
def test_event_detection_end_to_end_on_cleaned_dataset(tmp_path):
    """Verify end-to-end execution of Day 6 pipeline on the real cleaned dataset."""
    detector = StoreVisitDetector()
    summary = detector.run_detection_pipeline(output_dir=tmp_path)

    # 1. Statistical summary assertions
    assert summary.total_input_pings == 300
    assert summary.pings_in_catchments == 258
    assert summary.total_events_detected == 9
    assert summary.unique_devices_detected == 5
    assert summary.confirmed_store_visits > 0
    assert len(summary.stores_visited) == 5

    # 2. Dwell duration invariants
    dwell_stats = summary.dwell_time_summary_minutes
    assert dwell_stats["min_minutes"] >= 5.0
    assert dwell_stats["max_minutes"] <= 60.0
    assert dwell_stats["mean_minutes"] > 5.0

    # 3. Deliverable file persistence
    parquet_file = Path(summary.output_files["events_parquet"])
    csv_file = Path(summary.output_files["events_csv"])
    json_file = Path(summary.output_files["summary_json"])

    assert parquet_file.exists()
    assert csv_file.exists()
    assert json_file.exists()

    # 4. Load and verify Parquet and CSV
    df_parquet = pd.read_parquet(str(parquet_file))
    df_csv = pd.read_csv(str(csv_file))

    assert len(df_parquet) == 9
    assert len(df_csv) == 9
    assert "event_id" in df_parquet.columns
    assert "is_confirmed_visit" in df_parquet.columns
    assert "dwell_duration_minutes" in df_parquet.columns


def test_distinguish_catchment_presence_from_confirmed_visit():
    """Verify clear distinction between transit presence and confirmed retail visits."""
    detector = StoreVisitDetector(min_dwell_minutes=5.0, speed_threshold_mps=1.5)

    base_time = datetime(2026, 11, 26, 10, 0, 0, tzinfo=timezone.utc)

    # Device A: 10 pings, 10 minutes dwell, slow walking speed (0.5 m/s) -> Confirmed Visit
    pings_device_a = [
        {
            "device_id": "DEV_A",
            "store_id": "RET_MALL_FORUM_001",
            "store_name": "Forum Mall Koramangala",
            "category": "Shopping Mall",
            "timestamp_utc": (base_time + timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "speed_mps": 0.5,
            "buffer_radius_meters": 500.0,
        }
        for i in range(10)
    ]

    # Device B: 2 pings, 1 minute dwell, fast transit speed (8.0 m/s) -> Catchment Presence
    pings_device_b = [
        {
            "device_id": "DEV_B",
            "store_id": "RET_MALL_FORUM_001",
            "store_name": "Forum Mall Koramangala",
            "category": "Shopping Mall",
            "timestamp_utc": (base_time + timedelta(seconds=i * 60)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "speed_mps": 8.0,
            "buffer_radius_meters": 500.0,
        }
        for i in range(2)
    ]

    df_test = pd.DataFrame(pings_device_a + pings_device_b)
    events = detector.detect_continuous_events(df_test)

    assert len(events) == 2

    # Check Device A classification
    evt_a = next(e for e in events if e.device_id == "DEV_A")
    assert evt_a.event_type == "confirmed_store_visit"
    assert evt_a.is_confirmed_visit is True
    assert evt_a.dwell_duration_minutes == 9.0
    assert evt_a.ping_count == 10
    assert evt_a.avg_speed_mps == 0.5

    # Check Device B classification
    evt_b = next(e for e in events if e.device_id == "DEV_B")
    assert evt_b.event_type == "catchment_presence"
    assert evt_b.is_confirmed_visit is False
    assert evt_b.dwell_duration_minutes == 1.0
    assert evt_b.ping_count == 2
    assert evt_b.avg_speed_mps == 8.0


def test_single_ping_event_handling():
    """Verify single-ping detection produces valid non-negative event."""
    detector = StoreVisitDetector()

    single_ping_df = pd.DataFrame([
        {
            "device_id": "DEV_SINGLE",
            "store_id": "RET_CAFE_STARBUCKS_002",
            "store_name": "Starbucks 80ft Road",
            "category": "Cafe & Dining",
            "timestamp_utc": "2026-11-26T12:00:00Z",
            "speed_mps": 1.2,
            "buffer_radius_meters": 500.0,
        }
    ])

    events = detector.detect_continuous_events(single_ping_df)
    assert len(events) == 1

    evt = events[0]
    assert evt.device_id == "DEV_SINGLE"
    assert evt.entry_timestamp_utc == "2026-11-26T12:00:00Z"
    assert evt.exit_timestamp_utc == "2026-11-26T12:00:00Z"
    assert evt.dwell_duration_seconds == 0.0
    assert evt.dwell_duration_minutes == 0.0
    assert evt.ping_count == 1
    assert evt.event_type == "catchment_presence"
    assert evt.is_confirmed_visit is False


def test_session_split_on_large_time_gap():
    """Verify large time gaps split visits into distinct temporal sessions."""
    detector = StoreVisitDetector(max_session_gap_seconds=300)  # 5 min gap threshold

    t0 = datetime(2026, 11, 26, 8, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 11, 26, 17, 0, 0, tzinfo=timezone.utc)  # 9 hours later

    # Morning visit: 8:00 to 8:15 (4 pings at 5m intervals)
    morning_pings = [
        {
            "device_id": "DEV_COMMUTER",
            "store_id": "RET_CAFE_STARBUCKS_002",
            "store_name": "Starbucks 80ft Road",
            "category": "Cafe & Dining",
            "timestamp_utc": (t0 + timedelta(minutes=i * 5)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "speed_mps": 0.4,
            "buffer_radius_meters": 500.0,
        }
        for i in range(4)
    ]

    # Evening visit: 17:00 to 17:15 (4 pings at 5m intervals)
    evening_pings = [
        {
            "device_id": "DEV_COMMUTER",
            "store_id": "RET_CAFE_STARBUCKS_002",
            "store_name": "Starbucks 80ft Road",
            "category": "Cafe & Dining",
            "timestamp_utc": (t1 + timedelta(minutes=i * 5)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "speed_mps": 0.3,
            "buffer_radius_meters": 500.0,
        }
        for i in range(4)
    ]

    df_commuter = pd.DataFrame(morning_pings + evening_pings)
    events = detector.detect_continuous_events(df_commuter)

    # Must be split into 2 separate sessions
    assert len(events) == 2
    assert events[0].event_id != events[1].event_id
    assert events[0].entry_timestamp_utc.startswith("2026-11-26T08:00")
    assert events[1].entry_timestamp_utc.startswith("2026-11-26T17:00")
    assert events[0].is_confirmed_visit is True
    assert events[1].is_confirmed_visit is True


def test_validate_visit_events_rejects_negative_dwell():
    """Verify negative dwell duration or inverted timestamps fail validation."""
    bad_event = StoreVisitEvent(
        event_id="EVT_BAD_01",
        device_id="DEV_01",
        trajectory_id="TRJ_DEV_01",
        store_id="STORE_01",
        store_name="Store 1",
        store_category="Retail",
        entry_timestamp_utc="2026-11-26T12:00:00Z",
        exit_timestamp_utc="2026-11-26T11:00:00Z",  # Exits before entry!
        dwell_duration_seconds=-3600.0,
        dwell_duration_minutes=-60.0,
        ping_count=5,
        avg_speed_mps=1.0,
        max_speed_mps=2.0,
        event_type="confirmed_store_visit",
        is_confirmed_visit=True,
    )

    is_valid, errors = validate_visit_events([bad_event])
    assert is_valid is False
    assert any("Negative dwell duration" in e for e in errors)
    assert any("Exit timestamp" in e for e in errors)


def test_validate_visit_events_rejects_duplicate_event_ids():
    """Verify duplicate event IDs fail validation."""
    evt1 = StoreVisitEvent(
        event_id="EVT_DUPLICATE",
        device_id="DEV_01",
        trajectory_id="TRJ_DEV_01",
        store_id="STORE_01",
        store_name="Store 1",
        store_category="Retail",
        entry_timestamp_utc="2026-11-26T12:00:00Z",
        exit_timestamp_utc="2026-11-26T12:10:00Z",
        dwell_duration_seconds=600.0,
        dwell_duration_minutes=10.0,
        ping_count=5,
        avg_speed_mps=0.8,
        max_speed_mps=1.5,
        event_type="confirmed_store_visit",
        is_confirmed_visit=True,
    )
    evt2 = StoreVisitEvent(
        event_id="EVT_DUPLICATE",  # Same ID!
        device_id="DEV_02",
        trajectory_id="TRJ_DEV_02",
        store_id="STORE_02",
        store_name="Store 2",
        store_category="Retail",
        entry_timestamp_utc="2026-11-26T13:00:00Z",
        exit_timestamp_utc="2026-11-26T13:10:00Z",
        dwell_duration_seconds=600.0,
        dwell_duration_minutes=10.0,
        ping_count=5,
        avg_speed_mps=0.8,
        max_speed_mps=1.5,
        event_type="confirmed_store_visit",
        is_confirmed_visit=True,
    )

    is_valid, errors = validate_visit_events([evt1, evt2])
    assert is_valid is False
    assert any("Duplicate event_id detected" in e for e in errors)


def test_validate_input_pings_detects_corrupt_data():
    """Verify telemetry validator catches missing columns, nulls, and bad coords."""
    # 1. Missing mandatory column
    df_missing_col = pd.DataFrame({"ping_id": ["p1"], "latitude": [12.93]})
    valid, errs = validate_input_pings(df_missing_col)
    assert valid is False
    assert any("Missing mandatory column" in e for e in errs)

    # 2. Out of bounds coordinates
    df_bad_coords = pd.DataFrame({
        "ping_id": ["p1", "p2"],
        "device_id": ["d1", "d2"],
        "timestamp_utc": ["2026-11-26T10:00:00Z", "2026-11-26T10:01:00Z"],
        "latitude": [120.0, 12.93],    # 120.0 is invalid
        "longitude": [77.62, -200.0],  # -200.0 is invalid
    })
    valid, errs = validate_input_pings(df_bad_coords)
    assert valid is False
    assert any("latitude values outside" in e for e in errs)
    assert any("longitude values outside" in e for e in errs)


@pytest.mark.skipif(not GEOPANDAS_AVAILABLE, reason="geopandas is not installed in the active environment")
def test_mobility_metrics_calculator_calculate_dwell_times():
    """Verify MobilityMetricsCalculator computes dwell times via spatial join DataFrame."""
    cleaned_csv = get_project_root() / "data" / "processed" / "cleaned_gps_data.csv"
    engine = SedonaSpatialJoinEngine()
    joined_df = engine.join_pings_with_catchments(pings_data=cleaned_csv)

    calculator = MobilityMetricsCalculator(min_dwell_minutes=5.0)
    dwell_df = calculator.calculate_dwell_times(joined_df)

    assert len(dwell_df) == 9
    assert "dwell_duration_minutes" in dwell_df.columns
    assert "is_confirmed_visit" in dwell_df.columns
    assert all(dwell_df["dwell_duration_minutes"] >= 5.0)
