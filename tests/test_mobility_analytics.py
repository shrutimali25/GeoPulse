"""Unit tests for Day 7: Store Visit Analytics & Mobility Insights.

Verifies:
- Store-level visit counts, confirmed visits, device reach, and dwell metrics
- Device-level mobility profiles, store diversity, and temporal boundaries
- Hourly visit distributions and peak hour detection across multi-hour profiles
- Rigorous data validation: missing columns, null values, negative dwells, inverted timestamps, duplicate IDs
- Graceful handling of empty datasets, single-event datasets, and missing optional fields
- End-to-end file persistence and JSON summary integrity
- Class-based interface via MobilityMetricsCalculator and StoreMobilityAnalytics
"""

from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import pytest
import pandas as pd

from src.common.config_loader import get_project_root
from src.member1_data_eng.spatial_processing.mobility_metrics import (
    MobilityMetricsCalculator,
    StoreMobilityAnalytics,
    compute_device_mobility_analytics,
    compute_hourly_distribution,
    compute_hourly_visit_analytics,
    compute_store_visit_analytics,
    generate_analytics_summary,
    run_analytics_pipeline,
    validate_visit_events_dataframe,
)


@pytest.fixture
def sample_events_df():
    """Load the real Day 6 detected store catchment events."""
    events_csv = get_project_root() / "data" / "processed" / "store_catchment_events.csv"
    assert events_csv.exists(), f"Events CSV missing at {events_csv}"
    return pd.read_csv(events_csv)


@pytest.fixture
def synthetic_multi_hour_df():
    """Create a synthetic multi-hour visit event dataset for peak hour and edge case testing."""
    records = [
        # Store 1: Forum Mall - 2 visits at hour 10, 3 visits at hour 18
        {
            "event_id": "EVT_SYN_01",
            "device_id": "DEV_A",
            "trajectory_id": "TRJ_A",
            "store_id": "RET_MALL_FORUM_001",
            "store_name": "Forum Mall Koramangala",
            "store_category": "Shopping Mall",
            "entry_timestamp_utc": "2026-11-20T10:00:00Z",
            "exit_timestamp_utc": "2026-11-20T10:20:00Z",
            "dwell_duration_seconds": 1200.0,
            "dwell_duration_minutes": 20.0,
            "ping_count": 40,
            "avg_speed_mps": 0.4,
            "max_speed_mps": 1.2,
            "event_type": "confirmed_store_visit",
            "is_confirmed_visit": True,
            "buffer_radius_meters": 500.0,
        },
        {
            "event_id": "EVT_SYN_02",
            "device_id": "DEV_B",
            "trajectory_id": "TRJ_B",
            "store_id": "RET_MALL_FORUM_001",
            "store_name": "Forum Mall Koramangala",
            "store_category": "Shopping Mall",
            "entry_timestamp_utc": "2026-11-20T10:30:00Z",
            "exit_timestamp_utc": "2026-11-20T10:45:00Z",
            "dwell_duration_seconds": 900.0,
            "dwell_duration_minutes": 15.0,
            "ping_count": 30,
            "avg_speed_mps": 0.5,
            "max_speed_mps": 1.1,
            "event_type": "confirmed_store_visit",
            "is_confirmed_visit": True,
            "buffer_radius_meters": 500.0,
        },
        # Evening visits at 18:00 (peak hour)
        {
            "event_id": "EVT_SYN_03",
            "device_id": "DEV_A",
            "trajectory_id": "TRJ_A",
            "store_id": "RET_CAFE_STARBUCKS_002",
            "store_name": "Starbucks 80ft Road",
            "store_category": "Cafe & Dining",
            "entry_timestamp_utc": "2026-11-20T18:00:00Z",
            "exit_timestamp_utc": "2026-11-20T18:30:00Z",
            "dwell_duration_seconds": 1800.0,
            "dwell_duration_minutes": 30.0,
            "ping_count": 60,
            "avg_speed_mps": 0.3,
            "max_speed_mps": 0.8,
            "event_type": "confirmed_store_visit",
            "is_confirmed_visit": True,
            "buffer_radius_meters": 500.0,
        },
        {
            "event_id": "EVT_SYN_04",
            "device_id": "DEV_B",
            "trajectory_id": "TRJ_B",
            "store_id": "RET_CAFE_STARBUCKS_002",
            "store_name": "Starbucks 80ft Road",
            "store_category": "Cafe & Dining",
            "entry_timestamp_utc": "2026-11-20T18:15:00Z",
            "exit_timestamp_utc": "2026-11-20T18:45:00Z",
            "dwell_duration_seconds": 1800.0,
            "dwell_duration_minutes": 30.0,
            "ping_count": 60,
            "avg_speed_mps": 0.3,
            "max_speed_mps": 0.9,
            "event_type": "confirmed_store_visit",
            "is_confirmed_visit": True,
            "buffer_radius_meters": 500.0,
        },
        {
            "event_id": "EVT_SYN_05",
            "device_id": "DEV_C",
            "trajectory_id": "TRJ_C",
            "store_id": "RET_ELEC_CROMA_003",
            "store_name": "Croma Electronics 100ft Rd",
            "store_category": "Electronics",
            "entry_timestamp_utc": "2026-11-20T18:10:00Z",
            "exit_timestamp_utc": "2026-11-20T18:14:00Z",
            "dwell_duration_seconds": 240.0,
            "dwell_duration_minutes": 4.0,
            "ping_count": 8,
            "avg_speed_mps": 1.4,
            "max_speed_mps": 1.8,
            "event_type": "catchment_presence",
            "is_confirmed_visit": False,  # Transit / pass-through
            "buffer_radius_meters": 500.0,
        },
    ]
    return pd.DataFrame(records)


def test_store_visit_analytics_aggregation(sample_events_df):
    """Verify store-wise visit counts, confirmed visits, device reach, and dwell metrics."""
    store_df = compute_store_visit_analytics(sample_events_df)

    # 1. Row count and columns
    assert len(store_df) == 5
    expected_cols = [
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
    for col in expected_cols:
        assert col in store_df.columns

    # 2. Total visits check
    assert store_df["total_detected_visits"].sum() == 9
    assert store_df["confirmed_visits"].sum() == 9
    assert store_df["catchment_presence_events"].sum() == 0

    # 3. Store specific values
    # Croma should be top visited with 3 visits across 3 devices
    croma = store_df[store_df["store_id"] == "RET_ELEC_CROMA_003"].iloc[0]
    assert croma["total_detected_visits"] == 3
    assert croma["unique_devices"] == 3
    assert croma["store_category"] == "Electronics"
    assert croma["total_dwell_minutes"] == 38.0
    assert croma["avg_dwell_minutes"] == 12.67

    # Starbucks should have highest average dwell duration
    starbucks = store_df[store_df["store_id"] == "RET_CAFE_STARBUCKS_002"].iloc[0]
    assert starbucks["total_detected_visits"] == 2
    assert starbucks["unique_devices"] == 2
    assert starbucks["avg_dwell_minutes"] == 26.25
    assert starbucks["max_dwell_minutes"] == 29.5
    assert starbucks["min_dwell_minutes"] == 23.0

    # 4. Sorting: top visits first
    assert store_df.iloc[0]["total_detected_visits"] >= store_df.iloc[-1]["total_detected_visits"]


def test_device_mobility_analytics_aggregation(sample_events_df):
    """Verify device-wise visit counts, store diversity, dwell times, and temporal bounds."""
    device_df = compute_device_mobility_analytics(sample_events_df)

    # 1. Total devices
    assert len(device_df) == 5
    expected_cols = [
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
    for col in expected_cols:
        assert col in device_df.columns

    # 2. Total visits across devices
    assert device_df["total_detected_visits"].sum() == 9
    assert device_df["confirmed_visits"].sum() == 9

    # 3. Check DEV_00000002 has highest mobility (3 unique stores)
    dev2 = device_df[device_df["device_id"] == "DEV_00000002"].iloc[0]
    assert dev2["total_detected_visits"] == 3
    assert dev2["unique_stores_visited"] == 3
    assert dev2["total_dwell_minutes"] == 16.0
    assert dev2["avg_dwell_minutes"] == 5.33
    assert dev2["first_visit_utc"] == "2026-11-17T00:02:00Z"
    assert dev2["last_visit_utc"] == "2026-11-17T00:29:30Z"

    # 4. Check DEV_00000003 had longest single dwell (29.5 min)
    dev3 = device_df[device_df["device_id"] == "DEV_00000003"].iloc[0]
    assert dev3["total_detected_visits"] == 1
    assert dev3["unique_stores_visited"] == 1
    assert dev3["total_dwell_minutes"] == 29.5
    assert dev3["avg_dwell_minutes"] == 29.5


def test_hourly_visit_analytics_and_peak_detection(synthetic_multi_hour_df):
    """Verify hour-of-day visit distribution and peak hour identification."""
    hourly_df = compute_hourly_visit_analytics(synthetic_multi_hour_df)

    assert len(hourly_df) > 0
    assert "hour_of_day_utc" in hourly_df.columns
    assert "is_peak_hour" in hourly_df.columns
    assert "visit_count" in hourly_df.columns

    # Hour 18 should be marked as peak hour (3 visits total vs 2 at hour 10)
    peak_rows = hourly_df[hourly_df["is_peak_hour"] == True]
    assert len(peak_rows) > 0
    assert all(peak_rows["hour_of_day_utc"] == 18)

    # Hour 10 should not be peak
    non_peak_rows = hourly_df[hourly_df["hour_of_day_utc"] == 10]
    assert all(non_peak_rows["is_peak_hour"] == False)

    # Check aggregate distribution function
    dist_df = compute_hourly_distribution(synthetic_multi_hour_df)
    assert len(dist_df) == 2  # hours 10 and 18
    h10 = dist_df[dist_df["hour_of_day_utc"] == 10].iloc[0]
    h18 = dist_df[dist_df["hour_of_day_utc"] == 18].iloc[0]
    assert h10["total_visits"] == 2
    assert h18["total_visits"] == 3
    assert bool(h18["is_peak_hour"]) is True
    assert bool(h10["is_peak_hour"]) is False


def test_validation_catches_missing_columns():
    """Verify validation detects missing mandatory fields."""
    bad_df = pd.DataFrame({
        "event_id": ["EVT_01"],
        "device_id": ["DEV_01"],
        # Missing store_id, entry_timestamp_utc, exit_timestamp_utc, dwell_duration_minutes, is_confirmed_visit
    })
    is_valid, errors = validate_visit_events_dataframe(bad_df)
    assert is_valid is False
    assert any("Missing mandatory columns" in e for e in errors)

    # Calling compute functions on invalid df raises ValueError
    with pytest.raises(ValueError, match="Invalid events DataFrame"):
        compute_store_visit_analytics(bad_df)

    with pytest.raises(ValueError, match="Invalid events DataFrame"):
        compute_device_mobility_analytics(bad_df)

    with pytest.raises(ValueError, match="Invalid events DataFrame"):
        compute_hourly_visit_analytics(bad_df)


def test_validation_catches_negative_dwell():
    """Verify negative dwell durations are rejected by validator."""
    bad_df = pd.DataFrame({
        "event_id": ["EVT_01"],
        "device_id": ["DEV_01"],
        "store_id": ["STORE_01"],
        "entry_timestamp_utc": ["2026-11-20T10:00:00Z"],
        "exit_timestamp_utc": ["2026-11-20T10:10:00Z"],
        "dwell_duration_minutes": [-10.0],  # Negative!
        "is_confirmed_visit": [True],
    })
    is_valid, errors = validate_visit_events_dataframe(bad_df)
    assert is_valid is False
    assert any("negative dwell_duration_minutes" in e for e in errors)


def test_validation_catches_inverted_timestamps():
    """Verify exit timestamp preceding entry timestamp is rejected."""
    bad_df = pd.DataFrame({
        "event_id": ["EVT_01"],
        "device_id": ["DEV_01"],
        "store_id": ["STORE_01"],
        "entry_timestamp_utc": ["2026-11-20T12:00:00Z"],
        "exit_timestamp_utc": ["2026-11-20T11:00:00Z"],  # Exits before entry!
        "dwell_duration_minutes": [10.0],
        "is_confirmed_visit": [True],
    })
    is_valid, errors = validate_visit_events_dataframe(bad_df)
    assert is_valid is False
    assert any("exit_timestamp is earlier than entry_timestamp" in e for e in errors)


def test_validation_catches_duplicate_event_ids():
    """Verify duplicate event IDs fail validation."""
    bad_df = pd.DataFrame({
        "event_id": ["EVT_DUP", "EVT_DUP"],  # Duplicate!
        "device_id": ["DEV_01", "DEV_02"],
        "store_id": ["STORE_01", "STORE_02"],
        "entry_timestamp_utc": ["2026-11-20T10:00:00Z", "2026-11-20T11:00:00Z"],
        "exit_timestamp_utc": ["2026-11-20T10:10:00Z", "2026-11-20T11:10:00Z"],
        "dwell_duration_minutes": [10.0, 10.0],
        "is_confirmed_visit": [True, True],
    })
    is_valid, errors = validate_visit_events_dataframe(bad_df)
    assert is_valid is False
    assert any("duplicate event_id" in e for e in errors)


def test_empty_dataframe_handling():
    """Verify empty input DataFrame returns schema-aligned empty outputs without crashing."""
    empty_df = pd.DataFrame(columns=[
        "event_id",
        "device_id",
        "store_id",
        "store_name",
        "store_category",
        "entry_timestamp_utc",
        "exit_timestamp_utc",
        "dwell_duration_minutes",
        "is_confirmed_visit",
    ])

    is_valid, errors = validate_visit_events_dataframe(empty_df)
    assert is_valid is True
    assert len(errors) == 0

    store_df = compute_store_visit_analytics(empty_df)
    assert len(store_df) == 0
    assert "store_id" in store_df.columns
    assert "total_detected_visits" in store_df.columns

    device_df = compute_device_mobility_analytics(empty_df)
    assert len(device_df) == 0
    assert "device_id" in device_df.columns
    assert "unique_stores_visited" in device_df.columns

    hourly_df = compute_hourly_visit_analytics(empty_df)
    assert len(hourly_df) == 0
    assert "hour_of_day_utc" in hourly_df.columns
    assert "is_peak_hour" in hourly_df.columns

    summary = generate_analytics_summary(empty_df, store_df, device_df, hourly_df)
    assert summary["overall_summary"]["total_detected_visits"] == 0
    assert summary["overall_summary"]["unique_devices"] == 0
    assert summary["hourly_insights"]["peak_visiting_hours_utc"] == []
    assert summary["analytics_metadata"]["validation_status"] == "EMPTY_DATASET"


def test_single_event_dataframe():
    """Verify calculations on a DataFrame with a single event."""
    single_df = pd.DataFrame([{
        "event_id": "EVT_SOLO_01",
        "device_id": "DEV_SOLO",
        "store_id": "STORE_SOLO",
        "store_name": "Solo Retail",
        "store_category": "Boutique",
        "entry_timestamp_utc": "2026-11-20T14:00:00Z",
        "exit_timestamp_utc": "2026-11-20T14:15:00Z",
        "dwell_duration_minutes": 15.0,
        "is_confirmed_visit": True,
    }])

    store_df = compute_store_visit_analytics(single_df)
    assert len(store_df) == 1
    assert store_df.iloc[0]["total_detected_visits"] == 1
    assert store_df.iloc[0]["confirmed_visits"] == 1
    assert store_df.iloc[0]["min_dwell_minutes"] == 15.0
    assert store_df.iloc[0]["max_dwell_minutes"] == 15.0
    assert store_df.iloc[0]["avg_dwell_minutes"] == 15.0

    dev_df = compute_device_mobility_analytics(single_df)
    assert len(dev_df) == 1
    assert dev_df.iloc[0]["unique_stores_visited"] == 1
    assert dev_df.iloc[0]["avg_dwell_minutes"] == 15.0

    hourly_df = compute_hourly_visit_analytics(single_df)
    assert len(hourly_df) == 1
    assert hourly_df.iloc[0]["hour_of_day_utc"] == 14
    assert bool(hourly_df.iloc[0]["is_peak_hour"]) is True


def test_run_analytics_pipeline_end_to_end(tmp_path, sample_events_df):
    """Verify end-to-end pipeline execution and file creation in a designated directory."""
    summary = run_analytics_pipeline(events_path=sample_events_df, output_dir=tmp_path)

    # 1. Check summary dictionary
    assert summary["analytics_metadata"]["validation_status"] == "PASSED"
    assert summary["overall_summary"]["total_detected_visits"] == 9
    assert summary["overall_summary"]["unique_devices"] == 5
    assert summary["overall_summary"]["unique_stores"] == 5
    assert summary["hourly_insights"]["peak_visiting_hours_utc"] == [0]

    # 2. Check deliverable files exist
    store_csv = tmp_path / "store_visit_analytics.csv"
    dev_csv = tmp_path / "device_mobility_analytics.csv"
    hourly_csv = tmp_path / "hourly_visit_analytics.csv"
    summary_json = tmp_path / "mobility_analytics_summary.json"

    assert store_csv.exists()
    assert dev_csv.exists()
    assert hourly_csv.exists()
    assert summary_json.exists()

    # 3. Read back CSVs and verify contents
    loaded_store_df = pd.read_csv(store_csv)
    assert len(loaded_store_df) == 5
    assert loaded_store_df["total_detected_visits"].sum() == 9

    loaded_dev_df = pd.read_csv(dev_csv)
    assert len(loaded_dev_df) == 5
    assert loaded_dev_df["total_detected_visits"].sum() == 9

    loaded_hourly_df = pd.read_csv(hourly_csv)
    assert len(loaded_hourly_df) == 5
    assert loaded_hourly_df["visit_count"].sum() == 9

    with open(summary_json, "r", encoding="utf-8") as f:
        loaded_summary = json.load(f)
    assert loaded_summary["overall_summary"]["total_detected_visits"] == 9
    assert len(loaded_summary["store_rankings"]["by_total_visits"]) == 5


def test_mobility_metrics_calculator_class_interface(sample_events_df):
    """Verify MobilityMetricsCalculator and StoreMobilityAnalytics class wrapper methods."""
    calc = MobilityMetricsCalculator()
    assert isinstance(calc, StoreMobilityAnalytics)

    is_valid, errs = calc.validate_events(sample_events_df)
    assert is_valid is True
    assert len(errs) == 0

    s_df = calc.compute_store_analytics(sample_events_df)
    d_df = calc.compute_device_analytics(sample_events_df)
    h_df = calc.compute_hourly_analytics(sample_events_df)
    dist_df = calc.compute_hourly_distribution(sample_events_df)

    assert len(s_df) == 5
    assert len(d_df) == 5
    assert len(h_df) == 5
    assert len(dist_df) == 1

    summary = calc.generate_summary(sample_events_df, s_df, d_df, h_df)
    assert summary["overall_summary"]["total_detected_visits"] == 9
