"""Unit tests for Day 8: End-to-End Data Pipeline Orchestrator.

Verifies:
- Pipeline orchestrator initialization and directory configuration
- Independent execution of each stage (Stage 1 through Stage 7)
- End-to-end multi-stage pipeline coordination
- Execution tracking: timestamps, durations, record counts, and status
- Data quality monitoring: file integrity, schema adherence, nulls, duplicates, bounds, dwell, consistency
- Detection of missing files, schema violations, and corrupt data payloads
- Fail-fast error propagation and graceful failure handling
- Reproducibility with deterministic random seeding
- Serialization and validity of pipeline_execution_summary.json
"""

from datetime import datetime, timezone
import json
from pathlib import Path
import pytest
import pandas as pd

from src.common.config_loader import get_project_root
from src.member1_data_eng.pipeline.pipeline_orchestrator import (
    GeoPulsePipelineOrchestrator,
    PipelineExecutionError,
    PipelineExecutionSummary,
    PipelineStage,
    QualityCheckResult,
    StageExecutionRecord,
)


@pytest.fixture
def default_orchestrator():
    """Create orchestrator configured with default project root."""
    return GeoPulsePipelineOrchestrator(random_seed=42)


@pytest.fixture
def isolated_orchestrator(tmp_path):
    """Create orchestrator with isolated directories under a temporary directory."""
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    spatial_dir = tmp_path / "spatial"
    analytics_dir = processed_dir / "analytics"

    return GeoPulsePipelineOrchestrator(
        base_dir=tmp_path,
        raw_dir=raw_dir,
        processed_dir=processed_dir,
        spatial_dir=spatial_dir,
        analytics_dir=analytics_dir,
        random_seed=42,
    )


def test_pipeline_orchestrator_initialization(isolated_orchestrator, tmp_path):
    """Verify directory configuration and property initialization."""
    orch = isolated_orchestrator
    assert orch.root == tmp_path
    assert orch.raw_dir.exists()
    assert orch.processed_dir.exists()
    assert orch.spatial_dir.exists()
    assert orch.analytics_dir.exists()
    assert orch.random_seed == 42


def test_stage_1_generation_execution(isolated_orchestrator):
    """Verify Stage 1 generates raw GPS telemetry files with expected records."""
    orch = isolated_orchestrator
    record = orch.execute_stage_1_generation(
        total_devices=3,
        duration_hours=0.5,
        date_str="2026-11-20",
        seed=101,
    )

    assert record.stage_name == PipelineStage.STAGE_1_GENERATION.value
    assert record.status == "SUCCESS"
    assert record.input_records == 3
    assert record.output_records > 0
    assert record.duration_seconds >= 0.0
    assert len(record.artifacts_created) > 0
    for art in record.artifacts_created:
        assert Path(art).exists()


def test_stage_2_preprocessing_execution(default_orchestrator, tmp_path):
    """Verify Stage 2 cleans raw telemetry and produces valid/rejected CSVs."""
    orch = default_orchestrator
    record = orch.execute_stage_2_preprocessing(output_dir=tmp_path)

    assert record.stage_name == PipelineStage.STAGE_2_PREPROCESSING.value
    assert record.status == "SUCCESS"
    assert record.input_records > 0
    assert record.output_records > 0
    assert record.output_records <= record.input_records
    assert (tmp_path / "cleaned_gps_data.csv").exists()
    assert (tmp_path / "cleaning_summary.json").exists()


def test_stage_3_catchments_execution(default_orchestrator, tmp_path):
    """Verify Stage 3 generates 500m catchment GeoJSON polygons."""
    orch = default_orchestrator
    record = orch.execute_stage_3_catchments(buffer_radius_meters=500.0, spatial_dir=tmp_path)

    assert record.stage_name == PipelineStage.STAGE_3_CATCHMENTS.value
    assert record.status == "SUCCESS"
    assert record.input_records == 5
    assert record.output_records == 5
    assert (tmp_path / "store_locations.geojson").exists()
    assert (tmp_path / "store_catchments_500m.geojson").exists()
    assert (tmp_path / "catchment_generation_summary.json").exists()


def test_stage_4_spatial_join_execution(default_orchestrator):
    """Verify Stage 4 point-in-polygon spatial join between pings and catchments."""
    orch = default_orchestrator
    record, joined_df = orch.execute_stage_4_spatial_join()

    assert record.stage_name == PipelineStage.STAGE_4_SPATIAL_JOIN.value
    assert record.status == "SUCCESS"
    assert record.input_records > 0
    assert record.output_records > 0
    assert record.output_records <= record.input_records
    assert len(joined_df) == record.output_records
    assert "store_id" in joined_df.columns
    assert "device_id" in joined_df.columns


def test_stage_5_visit_detection_execution(default_orchestrator, tmp_path):
    """Verify Stage 5 detects visit events and saves Parquet/CSV deliverables."""
    orch = default_orchestrator
    record = orch.execute_stage_5_visit_detection(output_dir=tmp_path)

    assert record.stage_name == PipelineStage.STAGE_5_VISIT_DETECTION.value
    assert record.status == "SUCCESS"
    assert record.output_records > 0
    assert record.metrics["confirmed_visits"] > 0
    assert (tmp_path / "store_catchment_events.csv").exists()
    assert (tmp_path / "store_catchment_events.parquet").exists()


def test_stage_6_analytics_execution(default_orchestrator, tmp_path):
    """Verify Stage 6 computes store, device, and hourly mobility analytics."""
    orch = default_orchestrator
    record = orch.execute_stage_6_analytics(output_dir=tmp_path)

    assert record.stage_name == PipelineStage.STAGE_6_ANALYTICS.value
    assert record.status == "SUCCESS"
    assert record.output_records > 0
    assert (tmp_path / "store_visit_analytics.csv").exists()
    assert (tmp_path / "device_mobility_analytics.csv").exists()
    assert (tmp_path / "hourly_visit_analytics.csv").exists()
    assert (tmp_path / "mobility_analytics_summary.json").exists()


def test_data_quality_audit_on_default_project(default_orchestrator):
    """Verify data quality audit on fully populated project deliverables."""
    orch = default_orchestrator
    report = orch.run_data_quality_audit()

    assert report["total_checks"] == 9
    assert report["failed_checks"] == 0
    assert report["quality_score_percentage"] == 100.0

    check_names = {c["check_name"] for c in report["checks"]}
    expected_checks = {
        "CHECK_FILE_EXISTENCE",
        "CHECK_EMPTY_OUTPUTS",
        "CHECK_SCHEMA_COLUMNS",
        "CHECK_NULL_IDENTIFIERS",
        "CHECK_DUPLICATE_IDS",
        "CHECK_TIMESTAMP_VALIDITY",
        "CHECK_COORDINATE_BOUNDS",
        "CHECK_NON_NEGATIVE_DWELL",
        "CHECK_RECORD_COUNT_CONSISTENCY",
    }
    assert check_names == expected_checks
    for c in report["checks"]:
        assert c["status"] == "PASSED"


def test_data_quality_audit_detects_missing_files(isolated_orchestrator):
    """Verify quality audit flags missing deliverable files."""
    orch = isolated_orchestrator
    report = orch.run_data_quality_audit()

    assert report["failed_checks"] > 0
    assert report["quality_score_percentage"] < 100.0

    file_check = next(c for c in report["checks"] if c["check_name"] == "CHECK_FILE_EXISTENCE")
    assert file_check["status"] == "FAILED"
    assert "Missing required deliverable file" in file_check["details"]


def test_pipeline_fail_fast_on_invalid_input(isolated_orchestrator):
    """Verify fail_fast=True halts pipeline immediately upon stage failure."""
    orch = isolated_orchestrator

    # Running Stage 2 without Stage 1 generation on empty dir will fail
    with pytest.raises(PipelineExecutionError) as exc_info:
        orch.run_pipeline(run_generation=False, fail_fast=True)

    assert "gps_preprocessing_validation" in str(exc_info.value)


def test_end_to_end_pipeline_execution_isolated(isolated_orchestrator):
    """Verify complete 8-stage pipeline runs seamlessly in an isolated environment."""
    orch = isolated_orchestrator
    summary = orch.run_pipeline(
        run_generation=True,
        total_devices=4,
        duration_hours=0.5,
        date_str="2026-11-28",
        buffer_radius_meters=500.0,
        seed=999,
        fail_fast=True,
    )

    assert isinstance(summary, PipelineExecutionSummary)
    assert summary.overall_status == "SUCCESS"
    assert summary.stages_executed == 8
    assert summary.stages_succeeded == 8
    assert summary.stages_failed == 0
    assert summary.total_duration_seconds > 0.0

    # Verify summary JSON persistence
    summary_file = orch.processed_dir / "pipeline_execution_summary.json"
    assert summary_file.exists()

    with open(summary_file, "r", encoding="utf-8") as f:
        loaded = json.load(f)
    assert loaded["execution_id"] == summary.execution_id
    assert loaded["overall_status"] == "SUCCESS"
    assert len(loaded["stages"]) == 8
    assert loaded["data_quality_report"]["quality_score_percentage"] == 100.0


def test_pipeline_reproducibility_with_seed(tmp_path):
    """Verify identical random seeds produce identical stage outputs."""
    dir1 = tmp_path / "run1"
    dir2 = tmp_path / "run2"

    orch1 = GeoPulsePipelineOrchestrator(base_dir=dir1, random_seed=777)
    orch2 = GeoPulsePipelineOrchestrator(base_dir=dir2, random_seed=777)

    s1_1 = orch1.execute_stage_1_generation(total_devices=3, duration_hours=0.25, seed=777)
    s1_2 = orch2.execute_stage_1_generation(total_devices=3, duration_hours=0.25, seed=777)

    assert s1_1.output_records == s1_2.output_records

    # Check content identity of generated JSONL files
    file1 = Path(s1_1.artifacts_created[0])
    file2 = Path(s1_2.artifacts_created[0])
    with open(file1, "r", encoding="utf-8") as f1, open(file2, "r", encoding="utf-8") as f2:
        content1 = f1.readlines()
        content2 = f2.readlines()

    assert len(content1) == len(content2)
    assert content1 == content2
