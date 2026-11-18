"""GPS Data Cleaning and Preprocessing Pipeline.

Ingests raw GPS telemetry JSONL streams from data/raw/, validates schema and spatial bounds,
removes duplicates, normalizes timestamps to UTC, and outputs cleaned and rejected datasets.
"""

import csv
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from src.common.config_loader import get_project_root, load_yaml_config
from src.common.logger import get_logger
from src.member1_data_eng.validation.schema_validator import (
    DataQualityValidator,
    GPSPingRecord,
)

logger = get_logger(__name__)


@dataclass
class CleaningSummary:
    """Statistical summary of the GPS data cleaning pipeline."""

    total_input_records: int = 0
    valid_records: int = 0
    rejected_records: int = 0
    duplicate_records: int = 0
    missing_or_invalid_records: int = 0
    cleaning_success_percentage: float = 0.0
    rejection_breakdown: Dict[str, int] = field(default_factory=dict)
    files_processed: List[str] = field(default_factory=list)
    output_cleaned_file: Optional[str] = None
    output_rejected_file: Optional[str] = None
    output_summary_file: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert summary dataclass to dictionary."""
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        """Convert summary dataclass to JSON formatted string."""
        return json.dumps(self.to_dict(), indent=indent)


class GPSDataPreprocessor:
    """Preprocesses and cleans raw GPS telemetry files.

    Performs:
    1. Safe JSON parsing with malformed record handling.
    2. Required field presence and non-null validation.
    3. Timestamp normalization to UTC ISO-8601 strings.
    4. Numeric data type parsing and precision rounding.
    5. Schema and spatial bounds checking (using config-driven study zone).
    6. Duplicate detection (ping_id and device_id + timestamp).
    7. Cleaned CSV and rejected CSV export with audit logs.
    """

    REQUIRED_FIELDS: Tuple[str, ...] = (
        "ping_id",
        "device_id",
        "timestamp_utc",
        "latitude",
        "longitude",
        "horizontal_accuracy_meters",
        "speed_mps",
    )

    CLEANED_CSV_COLUMNS: List[str] = [
        "ping_id",
        "device_id",
        "timestamp_utc",
        "latitude",
        "longitude",
        "altitude_meters",
        "horizontal_accuracy_meters",
        "speed_mps",
        "heading_degrees",
        "is_synthetic",
        "profile_type",
    ]

    REJECTED_CSV_COLUMNS: List[str] = [
        "line_number",
        "file_source",
        "rejection_category",
        "rejection_reason",
        "raw_record",
    ]

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        validator: Optional[DataQualityValidator] = None,
    ):
        self.config = config or load_yaml_config()
        self.validator = validator or DataQualityValidator(config=self.config)
        logger.info(
            f"Initialized GPSDataPreprocessor with study zone bounds: "
            f"lat [{self.validator.min_lat}, {self.validator.max_lat}], "
            f"lon [{self.validator.min_lon}, {self.validator.max_lon}]"
        )

    def find_raw_files(self, raw_dir: Optional[Union[str, Path]] = None) -> List[Path]:
        """Locate all raw GPS JSONL files recursively under the given directory."""
        if raw_dir is None:
            raw_dir = get_project_root() / "data" / "raw"
        raw_path = Path(raw_dir)

        if not raw_path.exists():
            logger.warning(f"Raw data directory does not exist: {raw_path}")
            return []

        files = sorted(list(raw_path.rglob("*.jsonl")))
        logger.info(f"Found {len(files)} raw JSONL file(s) under {raw_path}")
        return files

    def normalize_timestamp(self, ts_raw: Any) -> Tuple[Optional[datetime], Optional[str]]:
        """Normalize various timestamp formats into a timezone-aware UTC datetime.

        Returns:
            Tuple of (datetime_obj, formatted_iso_string) or (None, None) if invalid.
        """
        if isinstance(ts_raw, datetime):
            if ts_raw.tzinfo is None:
                dt_utc = ts_raw.replace(tzinfo=timezone.utc)
            else:
                dt_utc = ts_raw.astimezone(timezone.utc)
            return dt_utc, dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        if isinstance(ts_raw, (int, float)):
            # Treat numeric as Unix epoch timestamp
            try:
                dt_utc = datetime.fromtimestamp(ts_raw, tz=timezone.utc)
                return dt_utc, dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                return None, None

        if isinstance(ts_raw, str):
            ts_str = ts_raw.strip()
            # Handle ISO formats including trailing Z
            if ts_str.endswith("Z"):
                ts_str = ts_str[:-1] + "+00:00"

            try:
                dt = datetime.fromisoformat(ts_str)
                if dt.tzinfo is None:
                    dt_utc = dt.replace(tzinfo=timezone.utc)
                else:
                    dt_utc = dt.astimezone(timezone.utc)
                return dt_utc, dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                # Fallback to dateutil if needed
                try:
                    from dateutil import parser
                    dt = parser.parse(ts_str)
                    if dt.tzinfo is None:
                        dt_utc = dt.replace(tzinfo=timezone.utc)
                    else:
                        dt_utc = dt.astimezone(timezone.utc)
                    return dt_utc, dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
                except Exception:
                    return None, None

        return None, None

    def parse_and_validate_line(
        self,
        line: str,
        line_num: int,
        file_path: str,
        seen_ping_ids: Set[str],
        seen_device_timestamps: Set[Tuple[str, str]],
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """Validate, clean, and deduplicate a single raw telemetry line.

        Returns:
            (cleaned_record, None) if valid.
            (None, rejection_dict) if invalid/rejected.
        """
        stripped_line = line.strip()
        if not stripped_line:
            # Skip empty lines cleanly
            return None, None

        # 1. Parse JSON safely
        try:
            record = json.loads(stripped_line)
            if not isinstance(record, dict):
                return None, {
                    "line_number": line_num,
                    "file_source": file_path,
                    "rejection_category": "MALFORMED_JSON",
                    "rejection_reason": "JSON root is not an object/dict",
                    "raw_record": stripped_line,
                }
        except json.JSONDecodeError as err:
            return None, {
                "line_number": line_num,
                "file_source": file_path,
                "rejection_category": "MALFORMED_JSON",
                "rejection_reason": f"Malformed JSON: {err.msg} at line {err.lineno} col {err.colno}",
                "raw_record": stripped_line,
            }

        # 2. Check required fields and missing/null values
        for req_field in self.REQUIRED_FIELDS:
            if req_field not in record or record[req_field] is None:
                return None, {
                    "line_number": line_num,
                    "file_source": file_path,
                    "rejection_category": "MISSING_REQUIRED_FIELD",
                    "rejection_reason": f"Missing or null required field: '{req_field}'",
                    "raw_record": stripped_line,
                }
            if isinstance(record[req_field], str) and not record[req_field].strip():
                return None, {
                    "line_number": line_num,
                    "file_source": file_path,
                    "rejection_category": "MISSING_REQUIRED_FIELD",
                    "rejection_reason": f"Empty string for required field: '{req_field}'",
                    "raw_record": stripped_line,
                }

        # 3. Check duplicate ping ID
        ping_id = str(record["ping_id"]).strip()
        if ping_id in seen_ping_ids:
            return None, {
                "line_number": line_num,
                "file_source": file_path,
                "rejection_category": "DUPLICATE_RECORD",
                "rejection_reason": f"Duplicate ping_id detected: '{ping_id}'",
                "raw_record": stripped_line,
            }

        # 4. Normalize and validate timestamp
        dt_utc, ts_normalized_str = self.normalize_timestamp(record["timestamp_utc"])
        if dt_utc is None or ts_normalized_str is None:
            return None, {
                "line_number": line_num,
                "file_source": file_path,
                "rejection_category": "INVALID_TIMESTAMP",
                "rejection_reason": f"Unparseable timestamp: '{record.get('timestamp_utc')}'",
                "raw_record": stripped_line,
            }

        # 5. Check duplicate device telemetry (same device_id and timestamp_utc)
        device_id = str(record["device_id"]).strip()
        device_ts_key = (device_id, ts_normalized_str)
        if device_ts_key in seen_device_timestamps:
            return None, {
                "line_number": line_num,
                "file_source": file_path,
                "rejection_category": "DUPLICATE_RECORD",
                "rejection_reason": (
                    f"Duplicate telemetry reading for device '{device_id}' "
                    f"at timestamp '{ts_normalized_str}'"
                ),
                "raw_record": stripped_line,
            }

        # 6. Correct data types and conversions
        try:
            latitude = float(record["latitude"])
            longitude = float(record["longitude"])
            speed_mps = float(record["speed_mps"])
            accuracy_m = float(record["horizontal_accuracy_meters"])

            altitude_m = (
                float(record["altitude_meters"])
                if record.get("altitude_meters") is not None
                else None
            )
            heading_deg = (
                float(record["heading_degrees"])
                if record.get("heading_degrees") is not None
                else None
            )
            is_synthetic = bool(record.get("is_synthetic", True))
            profile_type = str(record.get("profile_type", "unknown")).strip()

        except (ValueError, TypeError) as err:
            return None, {
                "line_number": line_num,
                "file_source": file_path,
                "rejection_category": "INVALID_DATA_TYPE",
                "rejection_reason": f"Data type casting error: {str(err)}",
                "raw_record": stripped_line,
            }

        # 7. Validate against Pydantic schema and dynamic study zone bounds
        validation_payload = {
            "ping_id": ping_id,
            "device_id": device_id,
            "timestamp_utc": dt_utc,
            "latitude": latitude,
            "longitude": longitude,
            "altitude_meters": altitude_m,
            "horizontal_accuracy_meters": accuracy_m,
            "speed_mps": speed_mps,
            "heading_degrees": heading_deg,
            "is_synthetic": is_synthetic,
        }

        is_valid, validation_err = self.validator.validate_record_dict(validation_payload)
        if not is_valid:
            category = (
                "OUT_OF_BOUNDS"
                if "outside study zone" in str(validation_err)
                else "SCHEMA_VALIDATION_ERROR"
            )
            return None, {
                "line_number": line_num,
                "file_source": file_path,
                "rejection_category": category,
                "rejection_reason": validation_err or "Failed schema validation",
                "raw_record": stripped_line,
            }

        # All checks passed: register in seen sets
        seen_ping_ids.add(ping_id)
        seen_device_timestamps.add(device_ts_key)

        cleaned_record = {
            "ping_id": ping_id,
            "device_id": device_id,
            "timestamp_utc": ts_normalized_str,
            "latitude": round(latitude, 6),
            "longitude": round(longitude, 6),
            "altitude_meters": round(altitude_m, 2) if altitude_m is not None else "",
            "horizontal_accuracy_meters": round(accuracy_m, 2),
            "speed_mps": round(speed_mps, 2),
            "heading_degrees": round(heading_deg, 2) if heading_deg is not None else "",
            "is_synthetic": is_synthetic,
            "profile_type": profile_type,
        }
        return cleaned_record, None

    def process_files(
        self,
        raw_files: Optional[List[Union[str, Path]]] = None,
        output_dir: Optional[Union[str, Path]] = None,
    ) -> CleaningSummary:
        """Process raw JSONL files, stream cleaned records to CSV, and log rejections."""
        if raw_files is None:
            files_to_process = self.find_raw_files()
        else:
            files_to_process = [Path(f) for f in raw_files]

        if output_dir is None:
            out_path = get_project_root() / "data" / "processed"
        else:
            out_path = Path(output_dir)

        out_path.mkdir(parents=True, exist_ok=True)
        cleaned_csv_path = out_path / "cleaned_gps_data.csv"
        rejected_csv_path = out_path / "rejected_gps_data.csv"
        summary_json_path = out_path / "cleaning_summary.json"

        seen_ping_ids: Set[str] = set()
        seen_device_timestamps: Set[Tuple[str, str]] = set()

        total_input_count = 0
        valid_count = 0
        duplicate_count = 0
        missing_invalid_count = 0
        rejection_breakdown: Dict[str, int] = {}
        processed_file_names: List[str] = []

        # Open both output CSV writers
        with open(cleaned_csv_path, "w", newline="", encoding="utf-8") as f_clean, \
             open(rejected_csv_path, "w", newline="", encoding="utf-8") as f_rej:

            clean_writer = csv.DictWriter(f_clean, fieldnames=self.CLEANED_CSV_COLUMNS)
            clean_writer.writeheader()

            rej_writer = csv.DictWriter(f_rej, fieldnames=self.REJECTED_CSV_COLUMNS)
            rej_writer.writeheader()

            for file_path in files_to_process:
                if not file_path.exists():
                    logger.warning(f"File not found: {file_path}")
                    continue

                processed_file_names.append(str(file_path))
                logger.info(f"Processing raw telemetry file: {file_path.name}")

                with open(file_path, "r", encoding="utf-8") as f_in:
                    for line_idx, line in enumerate(f_in, start=1):
                        if not line.strip():
                            continue

                        total_input_count += 1
                        cleaned, rejected = self.parse_and_validate_line(
                            line=line,
                            line_num=line_idx,
                            file_path=str(file_path),
                            seen_ping_ids=seen_ping_ids,
                            seen_device_timestamps=seen_device_timestamps,
                        )

                        if cleaned is not None:
                            valid_count += 1
                            clean_writer.writerow(cleaned)
                        elif rejected is not None:
                            category = rejected["rejection_category"]
                            rejection_breakdown[category] = rejection_breakdown.get(category, 0) + 1

                            if category == "DUPLICATE_RECORD":
                                duplicate_count += 1
                            else:
                                missing_invalid_count += 1

                            rej_writer.writerow(rejected)

        rejected_count = duplicate_count + missing_invalid_count
        success_pct = (
            round((valid_count / total_input_count) * 100.0, 2)
            if total_input_count > 0
            else 0.0
        )

        summary = CleaningSummary(
            total_input_records=total_input_count,
            valid_records=valid_count,
            rejected_records=rejected_count,
            duplicate_records=duplicate_count,
            missing_or_invalid_records=missing_invalid_count,
            cleaning_success_percentage=success_pct,
            rejection_breakdown=rejection_breakdown,
            files_processed=processed_file_names,
            output_cleaned_file=str(cleaned_csv_path),
            output_rejected_file=str(rejected_csv_path),
            output_summary_file=str(summary_json_path),
        )

        # Write summary JSON
        with open(summary_json_path, "w", encoding="utf-8") as f_sum:
            f_sum.write(summary.to_json(indent=2))

        logger.info(
            f"Data cleaning finished: {valid_count:,}/{total_input_count:,} valid "
            f"({success_pct}% success rate). Cleaned data saved to {cleaned_csv_path.name}"
        )
        return summary


def run_preprocessing_pipeline() -> CleaningSummary:
    """Execute the end-to-end preprocessing pipeline on data/raw/."""
    preprocessor = GPSDataPreprocessor()
    return preprocessor.process_files()


if __name__ == "__main__":
    summary = run_preprocessing_pipeline()
    print("\n================ GeoPulse Data Cleaning Summary ================")
    print(f"Total Input Records:         {summary.total_input_records:,}")
    print(f"Valid Records:               {summary.valid_records:,}")
    print(f"Rejected Records:            {summary.rejected_records:,}")
    print(f"  - Duplicate Records:       {summary.duplicate_records:,}")
    print(f"  - Missing/Invalid Records: {summary.missing_or_invalid_records:,}")
    print(f"Cleaning Success Rate:       {summary.cleaning_success_percentage:.2f}%")
    print(f"Rejection Breakdown:         {summary.rejection_breakdown}")
    print(f"Cleaned CSV Output:          {summary.output_cleaned_file}")
    print(f"Rejected CSV Output:         {summary.output_rejected_file}")
    print(f"Summary JSON:                {summary.output_summary_file}")
    print("================================================================\n")
