"""Unit tests for PySpark processing module.

Validates:
- Schema definition and field type mapping
- Required column presence in cleaned GPS data
- Metrics calculation logic and aggregation invariants
- Graceful error handling and diagnostic reporting when PySpark dependencies are missing
"""

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import pytest

from src.common.config_loader import get_project_root, load_yaml_config
from src.member1_data_eng.spatial_processing.pyspark_processor import (
    GPS_TELEMETRY_SCHEMA_DICT,
    JAVA_21_JVM_FLAGS,
    PYSPARK_AVAILABLE,
    REQUIRED_COLUMNS,
    ProcessingMetrics,
    PySparkGPSProcessor,
)


def test_schema_definition_and_required_columns():
    """Verify that explicit schema maps all required telemetry fields."""
    assert len(GPS_TELEMETRY_SCHEMA_DICT) == 11
    assert "ping_id" in GPS_TELEMETRY_SCHEMA_DICT
    assert "device_id" in GPS_TELEMETRY_SCHEMA_DICT
    assert "timestamp_utc" in GPS_TELEMETRY_SCHEMA_DICT
    assert "latitude" in GPS_TELEMETRY_SCHEMA_DICT
    assert "longitude" in GPS_TELEMETRY_SCHEMA_DICT
    assert "speed_mps" in GPS_TELEMETRY_SCHEMA_DICT
    assert "is_synthetic" in GPS_TELEMETRY_SCHEMA_DICT
    assert "profile_type" in GPS_TELEMETRY_SCHEMA_DICT

    for col in REQUIRED_COLUMNS:
        assert col in GPS_TELEMETRY_SCHEMA_DICT


def test_java_21_jvm_flags_present():
    """Verify essential JVM --add-opens flags are configured for Java 21."""
    assert len(JAVA_21_JVM_FLAGS) >= 8
    assert any("--add-opens=java.base/java.lang=ALL-UNNAMED" in flag for flag in JAVA_21_JVM_FLAGS)
    assert any("--add-opens=java.base/java.nio=ALL-UNNAMED" in flag for flag in JAVA_21_JVM_FLAGS)
    assert any("--add-opens=java.base/java.io=ALL-UNNAMED" in flag for flag in JAVA_21_JVM_FLAGS)


def test_processing_metrics_serialization():
    """Verify ProcessingMetrics dataclass serializes correctly."""
    metrics = ProcessingMetrics(
        total_pings=300,
        unique_devices=5,
        device_ping_counts={"DEV_00000001": 60, "DEV_00000002": 60},
        hourly_activity={0: 300},
        avg_speed_per_device={"DEV_00000001": 1.25},
        null_counts={"ping_id": 0, "latitude": 0},
        duplicate_pings_count=0,
    )

    d = metrics.to_dict()
    assert d["total_pings"] == 300
    assert d["unique_devices"] == 5
    assert "DEV_00000001" in d["device_ping_counts"]

    json_str = metrics.to_json()
    parsed = json.loads(json_str)
    assert parsed["total_pings"] == 300


def test_cleaned_dataset_matches_pyspark_schema():
    """Verify the real cleaned_gps_data.csv aligns with the PySpark schema."""
    csv_file = get_project_root() / "data" / "processed" / "cleaned_gps_data.csv"
    assert csv_file.exists(), f"Missing dataset: {csv_file}"

    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []

        # Check all required columns are in the CSV
        for col in REQUIRED_COLUMNS:
            assert col in headers, f"Missing required column in CSV: {col}"

        rows = list(reader)
        assert len(rows) > 0, "Cleaned dataset should not be empty"

        # Check types and non-null values
        for idx, row in enumerate(rows):
            for req in REQUIRED_COLUMNS:
                val = row.get(req)
                assert val is not None and val.strip() != "", (
                    f"Row {idx} has null/empty required field '{req}'"
                )

            # Check numeric validity
            lat = float(row["latitude"])
            lon = float(row["longitude"])
            speed = float(row["speed_mps"])
            acc = float(row["horizontal_accuracy_meters"])

            assert -90.0 <= lat <= 90.0
            assert -180.0 <= lon <= 180.0
            assert 0.0 <= speed <= 70.0
            assert 0.0 <= acc <= 100.0


def test_metrics_invariants_on_cleaned_data():
    """Verify distributed metrics mathematical invariants on the cleaned dataset."""
    csv_file = get_project_root() / "data" / "processed" / "cleaned_gps_data.csv"
    with open(csv_file, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    total_pings = len(rows)
    device_pings = {}
    hourly_counts = {}
    device_speeds = {}

    for row in rows:
        dev = row["device_id"]
        device_pings[dev] = device_pings.get(dev, 0) + 1

        speed = float(row["speed_mps"])
        if dev not in device_speeds:
            device_speeds[dev] = []
        device_speeds[dev].append(speed)

        # Parse hour
        ts_str = row["timestamp_utc"]
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1] + "+00:00"
        dt = datetime.fromisoformat(ts_str)
        hour = dt.hour
        hourly_counts[hour] = hourly_counts.get(hour, 0) + 1

    # Invariants
    unique_devices = len(device_pings)
    assert sum(device_pings.values()) == total_pings
    assert sum(hourly_counts.values()) == total_pings
    assert len(device_speeds) == unique_devices

    # Speed average validity
    for dev, speeds in device_speeds.items():
        avg_speed = sum(speeds) / len(speeds)
        assert 0.0 <= avg_speed <= 70.0


def test_pyspark_graceful_diagnostic_when_uninstalled():
    """Verify PySpark processor reports clear diagnostic if dependencies are not installed."""
    processor = PySparkGPSProcessor()
    schema = processor.get_explicit_schema()
    assert schema is not None

    if not PYSPARK_AVAILABLE:
        with pytest.raises(ImportError) as exc_info:
            processor.create_spark_session()
        assert "pyspark" in str(exc_info.value).lower()


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark is not installed in the environment")
def test_live_spark_processing_pipeline(tmp_path):
    """Live PySpark pipeline test (executed only when PySpark runtime is present)."""
    processor = PySparkGPSProcessor()
    spark = processor.create_spark_session()
    assert spark is not None

    csv_file = get_project_root() / "data" / "processed" / "cleaned_gps_data.csv"
    metrics = processor.run_pipeline(csv_path=csv_file, output_dir=tmp_path)

    assert metrics.total_pings == 300
    assert metrics.unique_devices == 5
    assert metrics.duplicate_pings_count == 0
    assert len(metrics.output_parquet_paths) > 0
