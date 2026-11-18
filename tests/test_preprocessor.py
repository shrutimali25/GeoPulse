"""Unit tests for the GPS Data Cleaning and Preprocessing pipeline.

Validates:
- Safe malformed JSON handling
- Missing and null required fields detection
- Dynamic study-zone bounds validation
- Duplicate ping ID and duplicate device-timestamp detection
- UTC timestamp normalization across varied formats
- Speed and GPS accuracy sanity checks
- Preservation of is_synthetic and profile_type
- End-to-end file processing and statistical summary accuracy
"""

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import pytest

from src.member1_data_eng.validation.preprocessor import (
    CleaningSummary,
    GPSDataPreprocessor,
)
from src.member1_data_eng.validation.schema_validator import DataQualityValidator


@pytest.fixture
def preprocessor():
    """Create a GPSDataPreprocessor with default project configuration."""
    return GPSDataPreprocessor()


def test_normalize_timestamp_variants(preprocessor):
    """Verify normalization of ISO strings, offsets, and invalid dates."""
    # UTC ISO string with Z
    dt, s = preprocessor.normalize_timestamp("2026-11-17T12:00:00Z")
    assert dt is not None
    assert s == "2026-11-17T12:00:00Z"
    assert dt.tzinfo == timezone.utc

    # ISO string with +05:30 offset (12:00:00+05:30 -> 06:30:00Z)
    dt_ist, s_ist = preprocessor.normalize_timestamp("2026-11-17T12:00:00+05:30")
    assert dt_ist is not None
    assert s_ist == "2026-11-17T06:30:00Z"

    # Unix timestamp numeric
    dt_num, s_num = preprocessor.normalize_timestamp(1763380800)
    assert dt_num is not None
    assert s_num is not None

    # Invalid timestamp string
    dt_bad, s_bad = preprocessor.normalize_timestamp("not-a-valid-timestamp")
    assert dt_bad is None
    assert s_bad is None


def test_malformed_json_handling(preprocessor):
    """Verify pipeline safely handles broken JSON syntax without crashing."""
    malformed_line = '{"ping_id": "123", "device_id": "DEV_00000001", broken...'
    cleaned, rejected = preprocessor.parse_and_validate_line(
        line=malformed_line,
        line_num=1,
        file_path="test_source.jsonl",
        seen_ping_ids=set(),
        seen_device_timestamps=set(),
    )

    assert cleaned is None
    assert rejected is not None
    assert rejected["rejection_category"] == "MALFORMED_JSON"
    assert "Malformed JSON" in rejected["rejection_reason"]
    assert rejected["raw_record"] == malformed_line


def test_missing_required_fields(preprocessor):
    """Verify missing or null required fields are rejected."""
    # Missing speed_mps
    record_missing_speed = {
        "ping_id": "c1a1a1a1-1111-2222-3333-444455556666",
        "device_id": "DEV_00000001",
        "timestamp_utc": "2026-11-17T08:00:00Z",
        "latitude": 12.9345,
        "longitude": 77.6250,
        "horizontal_accuracy_meters": 4.5,
    }

    cleaned, rejected = preprocessor.parse_and_validate_line(
        line=json.dumps(record_missing_speed),
        line_num=2,
        file_path="test_source.jsonl",
        seen_ping_ids=set(),
        seen_device_timestamps=set(),
    )

    assert cleaned is None
    assert rejected is not None
    assert rejected["rejection_category"] == "MISSING_REQUIRED_FIELD"
    assert "speed_mps" in rejected["rejection_reason"]


def test_out_of_bounds_rejection(preprocessor):
    """Verify coordinates outside configured Koramangala study zone are rejected."""
    # London coordinates
    record_out_of_bounds = {
        "ping_id": "c2a2a2a2-1111-2222-3333-444455556666",
        "device_id": "DEV_00000001",
        "timestamp_utc": "2026-11-17T08:00:00Z",
        "latitude": 51.5074,
        "longitude": -0.1278,
        "horizontal_accuracy_meters": 4.0,
        "speed_mps": 1.2,
    }

    cleaned, rejected = preprocessor.parse_and_validate_line(
        line=json.dumps(record_out_of_bounds),
        line_num=3,
        file_path="test_source.jsonl",
        seen_ping_ids=set(),
        seen_device_timestamps=set(),
    )

    assert cleaned is None
    assert rejected is not None
    assert rejected["rejection_category"] == "OUT_OF_BOUNDS"
    assert "outside study zone" in rejected["rejection_reason"]


def test_duplicate_ping_id_rejection(preprocessor):
    """Verify duplicate ping_id records are caught and rejected."""
    valid_record = {
        "ping_id": "dup-ping-0001-0002-0003-000400050006",
        "device_id": "DEV_00000001",
        "timestamp_utc": "2026-11-17T08:00:00Z",
        "latitude": 12.9345,
        "longitude": 77.6250,
        "horizontal_accuracy_meters": 3.8,
        "speed_mps": 1.1,
    }

    seen_pings = set()
    seen_dev_ts = set()

    # First encounter passes
    cleaned_1, rejected_1 = preprocessor.parse_and_validate_line(
        line=json.dumps(valid_record),
        line_num=4,
        file_path="test_source.jsonl",
        seen_ping_ids=seen_pings,
        seen_device_timestamps=seen_dev_ts,
    )
    assert cleaned_1 is not None
    assert rejected_1 is None

    # Second encounter with same ping_id is rejected
    cleaned_2, rejected_2 = preprocessor.parse_and_validate_line(
        line=json.dumps(valid_record),
        line_num=5,
        file_path="test_source.jsonl",
        seen_ping_ids=seen_pings,
        seen_device_timestamps=seen_dev_ts,
    )
    assert cleaned_2 is None
    assert rejected_2 is not None
    assert rejected_2["rejection_category"] == "DUPLICATE_RECORD"
    assert "Duplicate ping_id" in rejected_2["rejection_reason"]


def test_duplicate_device_timestamp_rejection(preprocessor):
    """Verify duplicate device telemetry at same timestamp is rejected even with different ping_id."""
    seen_pings = set()
    seen_dev_ts = set()

    record_1 = {
        "ping_id": "unique-ping-1111-2222-3333-444455556666",
        "device_id": "DEV_00000002",
        "timestamp_utc": "2026-11-17T09:00:00Z",
        "latitude": 12.9345,
        "longitude": 77.6250,
        "horizontal_accuracy_meters": 3.5,
        "speed_mps": 1.0,
    }

    record_2 = {
        "ping_id": "unique-ping-7777-8888-9999-000011112222",
        "device_id": "DEV_00000002",
        "timestamp_utc": "2026-11-17T09:00:00Z",  # Duplicate timestamp for DEV_00000002
        "latitude": 12.9346,
        "longitude": 77.6251,
        "horizontal_accuracy_meters": 4.0,
        "speed_mps": 1.2,
    }

    cleaned_1, _ = preprocessor.parse_and_validate_line(
        line=json.dumps(record_1),
        line_num=6,
        file_path="test.jsonl",
        seen_ping_ids=seen_pings,
        seen_device_timestamps=seen_dev_ts,
    )
    assert cleaned_1 is not None

    cleaned_2, rejected_2 = preprocessor.parse_and_validate_line(
        line=json.dumps(record_2),
        line_num=7,
        file_path="test.jsonl",
        seen_ping_ids=seen_pings,
        seen_device_timestamps=seen_dev_ts,
    )
    assert cleaned_2 is None
    assert rejected_2 is not None
    assert rejected_2["rejection_category"] == "DUPLICATE_RECORD"
    assert "Duplicate telemetry reading for device" in rejected_2["rejection_reason"]


def test_invalid_speed_and_accuracy(preprocessor):
    """Verify negative speed and excessive speed are rejected by schema validator."""
    # Negative speed
    rec_neg_speed = {
        "ping_id": "c3a3a3a3-1111-2222-3333-444455556666",
        "device_id": "DEV_00000001",
        "timestamp_utc": "2026-11-17T08:00:00Z",
        "latitude": 12.9345,
        "longitude": 77.6250,
        "horizontal_accuracy_meters": 4.0,
        "speed_mps": -5.0,
    }

    _, rej_neg = preprocessor.parse_and_validate_line(
        line=json.dumps(rec_neg_speed),
        line_num=8,
        file_path="test.jsonl",
        seen_ping_ids=set(),
        seen_device_timestamps=set(),
    )
    assert rej_neg is not None
    assert rej_neg["rejection_category"] == "SCHEMA_VALIDATION_ERROR"

    # Excessive speed (> 70 m/s)
    rec_hyper_speed = dict(rec_neg_speed)
    rec_hyper_speed["speed_mps"] = 150.0
    _, rej_hyper = preprocessor.parse_and_validate_line(
        line=json.dumps(rec_hyper_speed),
        line_num=9,
        file_path="test.jsonl",
        seen_ping_ids=set(),
        seen_device_timestamps=set(),
    )
    assert rej_hyper is not None
    assert rej_hyper["rejection_category"] == "SCHEMA_VALIDATION_ERROR"


def test_metadata_preservation(preprocessor):
    """Verify is_synthetic and profile_type are strictly preserved."""
    record = {
        "ping_id": "c4a4a4a4-1111-2222-3333-444455556666",
        "device_id": "DEV_00000001",
        "timestamp_utc": "2026-11-17T08:00:00Z",
        "latitude": 12.9345,
        "longitude": 77.6250,
        "altitude_meters": 921.4,
        "horizontal_accuracy_meters": 3.9,
        "speed_mps": 1.25,
        "heading_degrees": 180.0,
        "is_synthetic": True,
        "profile_type": "daily_shoppers",
    }

    cleaned, rej = preprocessor.parse_and_validate_line(
        line=json.dumps(record),
        line_num=10,
        file_path="test.jsonl",
        seen_ping_ids=set(),
        seen_device_timestamps=set(),
    )

    assert rej is None
    assert cleaned is not None
    assert cleaned["is_synthetic"] is True
    assert cleaned["profile_type"] == "daily_shoppers"
    assert cleaned["altitude_meters"] == 921.4
    assert cleaned["timestamp_utc"] == "2026-11-17T08:00:00Z"


def test_end_to_end_file_processing(tmp_path, preprocessor):
    """Verify complete batch processing, CSV output creation, and summary calculation."""
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    raw_dir.mkdir()

    raw_file = raw_dir / "telemetry_test.jsonl"

    test_lines = [
        # 1. Valid record 1
        json.dumps({
            "ping_id": "p001-0000-0000-0000-000000000001",
            "device_id": "DEV_00000001",
            "timestamp_utc": "2026-11-17T08:00:00Z",
            "latitude": 12.9345,
            "longitude": 77.6250,
            "altitude_meters": 920.0,
            "horizontal_accuracy_meters": 4.0,
            "speed_mps": 1.0,
            "is_synthetic": True,
            "profile_type": "daily_shoppers",
        }),
        # 2. Valid record 2
        json.dumps({
            "ping_id": "p002-0000-0000-0000-000000000002",
            "device_id": "DEV_00000001",
            "timestamp_utc": "2026-11-17T08:00:30Z",
            "latitude": 12.9348,
            "longitude": 77.6252,
            "altitude_meters": 920.1,
            "horizontal_accuracy_meters": 4.1,
            "speed_mps": 1.1,
            "is_synthetic": True,
            "profile_type": "daily_shoppers",
        }),
        # 3. Duplicate record (duplicate ping_id)
        json.dumps({
            "ping_id": "p001-0000-0000-0000-000000000001",
            "device_id": "DEV_00000002",
            "timestamp_utc": "2026-11-17T08:01:00Z",
            "latitude": 12.9345,
            "longitude": 77.6250,
            "horizontal_accuracy_meters": 4.0,
            "speed_mps": 1.0,
        }),
        # 4. Out of bounds record
        json.dumps({
            "ping_id": "p003-0000-0000-0000-000000000003",
            "device_id": "DEV_00000001",
            "timestamp_utc": "2026-11-17T08:01:30Z",
            "latitude": 0.0,
            "longitude": 0.0,
            "horizontal_accuracy_meters": 4.0,
            "speed_mps": 1.0,
        }),
        # 5. Missing required field (latitude)
        json.dumps({
            "ping_id": "p004-0000-0000-0000-000000000004",
            "device_id": "DEV_00000001",
            "timestamp_utc": "2026-11-17T08:02:00Z",
            "longitude": 77.6250,
            "horizontal_accuracy_meters": 4.0,
            "speed_mps": 1.0,
        }),
        # 6. Malformed JSON
        '{"ping_id": "broken", "device_id": corrupted line',
    ]

    with open(raw_file, "w", encoding="utf-8") as f:
        for line in test_lines:
            f.write(line + "\n")

    summary = preprocessor.process_files(
        raw_files=[raw_file],
        output_dir=processed_dir,
    )

    assert summary.total_input_records == 6
    assert summary.valid_records == 2
    assert summary.rejected_records == 4
    assert summary.duplicate_records == 1
    assert summary.missing_or_invalid_records == 3
    assert summary.cleaning_success_percentage == pytest.approx(33.33, rel=1e-2)

    # Check that cleaned CSV exists and has 2 records (+ 1 header)
    cleaned_csv = processed_dir / "cleaned_gps_data.csv"
    assert cleaned_csv.exists()
    with open(cleaned_csv, "r", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
        assert len(reader) == 2
        assert reader[0]["ping_id"] == "p001-0000-0000-0000-000000000001"
        assert reader[0]["is_synthetic"] == "True"
        assert reader[0]["profile_type"] == "daily_shoppers"

    # Check that rejected CSV exists and has 4 records
    rejected_csv = processed_dir / "rejected_gps_data.csv"
    assert rejected_csv.exists()
    with open(rejected_csv, "r", encoding="utf-8") as f:
        rej_reader = list(csv.DictReader(f))
        assert len(rej_reader) == 4
        categories = [r["rejection_category"] for r in rej_reader]
        assert "DUPLICATE_RECORD" in categories
        assert "OUT_OF_BOUNDS" in categories
        assert "MISSING_REQUIRED_FIELD" in categories
        assert "MALFORMED_JSON" in categories

    # Check summary JSON file
    summary_json = processed_dir / "cleaning_summary.json"
    assert summary_json.exists()
    with open(summary_json, "r", encoding="utf-8") as f:
        data = json.load(f)
        assert data["total_input_records"] == 6
        assert data["valid_records"] == 2
        assert data["rejected_records"] == 4
