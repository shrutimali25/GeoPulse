"""Schema validation and quality assurance for GPS mobility pings.

Implements strict data contracts using Pydantic v2 to ensure data hygiene
before spatial processing and Snowflake ingestion.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import uuid
import re

try:
    from pydantic import BaseModel, Field, field_validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False
    BaseModel = object  # type: ignore

from src.common.config_loader import load_yaml_config
from src.common.logger import get_logger

logger = get_logger(__name__)


if PYDANTIC_AVAILABLE:
    class GPSPingRecord(BaseModel):
        """Pydantic model representing a single validated GPS telemetry ping."""

        ping_id: str = Field(
            default_factory=lambda: str(uuid.uuid4()),
            description="Globally unique ping UUID",
        )
        device_id: str = Field(
            ...,
            pattern=r"^DEV_[A-Z0-9]{8}$",
            description="Anonymized persistent mobile device ID",
        )
        timestamp_utc: datetime = Field(
            ...,
            description="Timestamp of the ping in UTC",
        )
        latitude: float = Field(
            ...,
            ge=-90.0,
            le=90.0,
            description="WGS84 Latitude (-90 to 90)",
        )
        longitude: float = Field(
            ...,
            ge=-180.0,
            le=180.0,
            description="WGS84 Longitude (-180 to 180)",
        )
        altitude_meters: Optional[float] = Field(
            default=None,
            description="Estimated altitude above sea level in meters",
        )
        horizontal_accuracy_meters: float = Field(
            ...,
            ge=0.0,
            le=100.0,
            description="Horizontal accuracy (CEP radius in meters)",
        )
        speed_mps: float = Field(
            ...,
            ge=0.0,
            le=70.0,
            description="Instantaneous ground speed in meters per second (max ~250 km/h)",
        )
        heading_degrees: Optional[float] = Field(
            default=None,
            ge=0.0,
            le=360.0,
            description="Heading angle in degrees clockwise from true north",
        )
        is_synthetic: bool = Field(
            default=True,
            description="Flag identifying telemetry as synthetically simulated",
        )

        @field_validator("speed_mps")
        @classmethod
        def validate_speed_realism(cls, v: float) -> float:
            """Ensure speed is within physically plausible urban mobility limits."""
            if v < 0:
                raise ValueError("Speed cannot be negative")
            return v
else:
    class GPSPingRecord:  # type: ignore
        """Fallback mock class when Pydantic is not yet installed."""
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)


class DataQualityValidator:
    """Performs batch validation and bounding box enforcement on telemetry data."""

    def __init__(
        self,
        min_lat: Optional[float] = None,
        max_lat: Optional[float] = None,
        min_lon: Optional[float] = None,
        max_lon: Optional[float] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        if min_lat is None or max_lat is None or min_lon is None or max_lon is None:
            try:
                cfg = config or load_yaml_config()
                bbox = cfg.get("study_zone", {}).get("bounding_box", {})
                self.min_lat = min_lat if min_lat is not None else float(bbox.get("min_lat", 12.9200))
                self.max_lat = max_lat if max_lat is not None else float(bbox.get("max_lat", 12.9450))
                self.min_lon = min_lon if min_lon is not None else float(bbox.get("min_lon", 77.6100))
                self.max_lon = max_lon if max_lon is not None else float(bbox.get("max_lon", 77.6400))
            except Exception as e:
                logger.warning(f"Could not load study zone bounding box from config: {e}. Using defaults.")
                self.min_lat = min_lat if min_lat is not None else 12.9200
                self.max_lat = max_lat if max_lat is not None else 12.9450
                self.min_lon = min_lon if min_lon is not None else 77.6100
                self.max_lon = max_lon if max_lon is not None else 77.6400
        else:
            self.min_lat = min_lat
            self.max_lat = max_lat
            self.min_lon = min_lon
            self.max_lon = max_lon

    def is_within_study_zone(self, lat: float, lon: float) -> bool:
        """Check if coordinates fall within the spatial bounding box."""
        return self.min_lat <= lat <= self.max_lat and self.min_lon <= lon <= self.max_lon

    def validate_record_dict(self, record: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate a single raw record dict.

        Returns:
            Tuple of (is_valid, error_message)
        """
        if not PYDANTIC_AVAILABLE:
            # Fallback basic check
            if not (self.min_lat <= record.get("latitude", 0) <= self.max_lat):
                return False, "Latitude out of bounds"
            if not (self.min_lon <= record.get("longitude", 0) <= self.max_lon):
                return False, "Longitude out of bounds"
            return True, None

        try:
            validated = GPSPingRecord(**record)
            if not self.is_within_study_zone(validated.latitude, validated.longitude):
                return False, f"Coordinates ({validated.latitude}, {validated.longitude}) outside study zone"
            return True, None
        except Exception as e:
            return False, str(e)

    def validate_batch(
        self, records: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Validate a batch of raw records.

        Returns:
            Dictionary with counts of passed and failed records, and sample errors.
        """
        valid_records: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []

        for idx, rec in enumerate(records):
            is_valid, err = self.validate_record_dict(rec)
            if is_valid:
                valid_records.append(rec)
            else:
                errors.append({"record_index": idx, "error": err, "record": rec})

        logger.info(
            f"Validation complete: {len(valid_records)} valid, {len(errors)} rejected."
        )
        return {
            "total": len(records),
            "valid_count": len(valid_records),
            "invalid_count": len(errors),
            "valid_records": valid_records,
            "errors": errors,
        }
