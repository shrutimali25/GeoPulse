"""Tests for data validation and quality gates."""

from datetime import datetime, timezone
import pytest
from src.member1_data_eng.validation.schema_validator import (
    DataQualityValidator,
    GPSPingRecord,
    PYDANTIC_AVAILABLE,
)


def test_study_zone_bounds():
    """Verify coordinate bounding box validation."""
    validator = DataQualityValidator(
        min_lat=12.9200, max_lat=12.9450, min_lon=77.6100, max_lon=77.6400
    )

    # Point inside Koramangala
    assert validator.is_within_study_zone(12.9345, 77.6250) is True

    # Point outside (London coordinates)
    assert validator.is_within_study_zone(51.5074, -0.1278) is False

    # Edge cases
    assert validator.is_within_study_zone(12.9200, 77.6100) is True
    assert validator.is_within_study_zone(12.9199, 77.6100) is False


@pytest.mark.skipif(not PYDANTIC_AVAILABLE, reason="Pydantic not yet installed")
def test_valid_gps_ping_record():
    """Verify that a compliant ping passes Pydantic schema validation."""
    raw_record = {
        "device_id": "DEV_A1B2C3D4",
        "timestamp_utc": datetime.now(timezone.utc),
        "latitude": 12.9345,
        "longitude": 77.6250,
        "horizontal_accuracy_meters": 4.2,
        "speed_mps": 1.4,
        "heading_degrees": 90.0,
    }
    record = GPSPingRecord(**raw_record)
    assert record.device_id == "DEV_A1B2C3D4"
    assert record.latitude == 12.9345
    assert record.speed_mps == 1.4


@pytest.mark.skipif(not PYDANTIC_AVAILABLE, reason="Pydantic not yet installed")
def test_invalid_device_id_format():
    """Verify that malformed device IDs are rejected."""
    raw_record = {
        "device_id": "INVALID_ID_FORMAT",
        "timestamp_utc": datetime.now(timezone.utc),
        "latitude": 12.9345,
        "longitude": 77.6250,
        "horizontal_accuracy_meters": 4.2,
        "speed_mps": 1.4,
    }
    with pytest.raises(Exception):
        GPSPingRecord(**raw_record)


def test_batch_validation():
    """Verify batch processing divides clean and rejected pings."""
    validator = DataQualityValidator()
    records = [
        {
            "device_id": "DEV_A1B2C3D4",
            "timestamp_utc": datetime.now(timezone.utc),
            "latitude": 12.9345,
            "longitude": 77.6250,
            "horizontal_accuracy_meters": 4.0,
            "speed_mps": 1.5,
        },
        {
            "device_id": "DEV_A1B2C3D4",
            "timestamp_utc": datetime.now(timezone.utc),
            "latitude": 0.0,  # Far outside Koramangala
            "longitude": 0.0,
            "horizontal_accuracy_meters": 4.0,
            "speed_mps": 1.5,
        },
    ]
    results = validator.validate_batch(records)
    assert results["total"] == 2
    assert results["valid_count"] == 1
    assert results["invalid_count"] == 1
