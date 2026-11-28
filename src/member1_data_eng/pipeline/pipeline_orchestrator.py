"""End-to-End Data Pipeline Orchestrator for GeoPulse.

Integrates and orchestrates the complete data engineering workflow:
1. GPS Data Generation (stochastic mobility simulation)
2. GPS Data Preprocessing and Schema Validation
3. Store 500m Catchment Area Generation
4. Spatial Join Processing (Point-in-Polygon)
5. Store Visit & Catchment Presence Detection
6. Store-Level, Device-Level & Hourly Mobility Analytics
7. Data Quality Monitoring & Pipeline Execution Reporting
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
import json
import os
from pathlib import Path
import time
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = Any  # type: ignore

try:
    import geopandas as gpd
    GEOPANDAS_AVAILABLE = True
except ImportError:
    GEOPANDAS_AVAILABLE = False
    gpd = Any  # type: ignore

from src.common.config_loader import get_project_root, load_yaml_config
from src.common.logger import get_logger

logger = get_logger(__name__)


class PipelineStage(str, Enum):
    """Enumeration of pipeline execution stages."""

    STAGE_1_GENERATION = "gps_data_generation"
    STAGE_2_PREPROCESSING = "gps_preprocessing_validation"
    STAGE_3_CATCHMENTS = "store_catchment_generation"
    STAGE_4_SPATIAL_JOIN = "spatial_join_processing"
    STAGE_5_VISIT_DETECTION = "store_visit_detection"
    STAGE_6_ANALYTICS = "store_visit_analytics"
    STAGE_7_QUALITY_AUDIT = "data_quality_audit_summary"


class PipelineExecutionError(Exception):
    """Raised when a pipeline stage encounters a critical failure."""

    def __init__(self, stage_name: str, message: str, stage_record: Optional[Any] = None):
        super().__init__(f"Stage '{stage_name}' failed: {message}")
        self.stage_name = stage_name
        self.stage_record = stage_record


@dataclass
class StageExecutionRecord:
    """Detailed record of an individual pipeline stage execution."""

    stage_name: str
    stage_order: int
    start_time_utc: str
    end_time_utc: str
    duration_seconds: float
    status: str  # "SUCCESS" or "FAILED"
    input_records: int
    output_records: int
    artifacts_created: List[str] = field(default_factory=list)
    error_message: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert record to dictionary."""
        return asdict(self)


@dataclass
class QualityCheckResult:
    """Record of an individual data quality check."""

    check_name: str
    category: str  # "FILE_INTEGRITY", "SCHEMA", "NULLS", "DUPLICATES", "BOUNDS", "CONSISTENCY"
    status: str  # "PASSED", "FAILED", "WARNING"
    details: str
    records_checked: int = 0
    records_failed: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return asdict(self)


@dataclass
class PipelineExecutionSummary:
    """Comprehensive summary of the complete pipeline execution."""

    execution_id: str
    execution_start_utc: str
    execution_end_utc: str
    total_duration_seconds: float
    overall_status: str  # "SUCCESS" or "FAILED"
    stages_executed: int
    stages_succeeded: int
    stages_failed: int
    stages: List[Dict[str, Any]]
    data_quality_report: Dict[str, Any]
    output_artifacts: Dict[str, str]
    pipeline_configuration: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert summary to dictionary."""
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        """Serialize summary to formatted JSON string."""
        return json.dumps(self.to_dict(), indent=indent)


class GeoPulsePipelineOrchestrator:
    """Orchestrates end-to-end GeoPulse data engineering workflows.

    Coordinates data generation, preprocessing, spatial joins, visit detection,
    mobility analytics, and automated quality monitoring through a unified entry point.
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        base_dir: Optional[Union[str, Path]] = None,
        raw_dir: Optional[Union[str, Path]] = None,
        processed_dir: Optional[Union[str, Path]] = None,
        spatial_dir: Optional[Union[str, Path]] = None,
        analytics_dir: Optional[Union[str, Path]] = None,
        random_seed: int = 42,
    ):
        """Initialize the pipeline orchestrator.

        Args:
            config: Base YAML configuration dictionary.
            base_dir: Project root directory.
            raw_dir: Directory containing or receiving raw GPS telemetry.
            processed_dir: Directory for processed datasets.
            spatial_dir: Directory for spatial boundary and catchment GeoJSONs.
            analytics_dir: Directory for business mobility metrics.
            random_seed: Random seed for deterministic reproducibility.
        """
        self.root = Path(base_dir) if base_dir else get_project_root()
        self.config = config or load_yaml_config()
        self.random_seed = random_seed

        # Configure directories
        self.raw_dir = Path(raw_dir) if raw_dir else self.root / "data" / "raw"
        self.processed_dir = Path(processed_dir) if processed_dir else self.root / "data" / "processed"
        self.spatial_dir = Path(spatial_dir) if spatial_dir else self.root / "data" / "spatial"
        self.analytics_dir = Path(analytics_dir) if analytics_dir else self.processed_dir / "analytics"

        # Ensure directories exist
        for d in [self.raw_dir, self.processed_dir, self.spatial_dir, self.analytics_dir]:
            d.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Initialized GeoPulsePipelineOrchestrator at {self.root} "
            f"(seed: {self.random_seed})."
        )

    # -------------------------------------------------------------------------
    # STAGE 1: GPS Data Generation
    # -------------------------------------------------------------------------
    def execute_stage_1_generation(
        self,
        total_devices: int = 5,
        duration_hours: float = 0.5,
        date_str: str = "2026-11-17",
        output_dir: Optional[Union[str, Path]] = None,
        seed: Optional[int] = None,
    ) -> StageExecutionRecord:
        """Execute Stage 1: Synthetic GPS Trajectory Generation.

        Args:
            total_devices: Number of mobile devices to simulate.
            duration_hours: Trajectory duration in hours.
            date_str: Partition date string for raw telemetry directory.
            output_dir: Destination directory for raw telemetry files.
            seed: Override random seed.

        Returns:
            StageExecutionRecord detailing execution metrics.
        """
        stage_name = PipelineStage.STAGE_1_GENERATION.value
        start_ts = datetime.now(timezone.utc)
        logger.info(f"--- Starting Stage 1: {stage_name} ---")

        effective_seed = seed if seed is not None else self.random_seed
        target_dir = Path(output_dir) if output_dir else self.raw_dir / f"date={date_str}"
        target_dir.mkdir(parents=True, exist_ok=True)

        try:
            try:
                dt_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except Exception:
                dt_date = datetime.now(timezone.utc)

            from src.member1_data_eng.generator.trajectory_simulator import TrajectorySimulator

            simulator = TrajectorySimulator(config=self.config, seed=effective_seed)
            gen_summary = simulator.run_simulation(
                start_time=dt_date,
                device_count=total_devices,
                duration_hours=duration_hours,
                output_dir=str(target_dir),
                output_format="jsonl",
            )

            end_ts = datetime.now(timezone.utc)
            duration = (end_ts - start_ts).total_seconds()
            artifacts = gen_summary.get("files_written", [])

            record = StageExecutionRecord(
                stage_name=stage_name,
                stage_order=1,
                start_time_utc=start_ts.isoformat(),
                end_time_utc=end_ts.isoformat(),
                duration_seconds=round(duration, 3),
                status="SUCCESS",
                input_records=total_devices,
                output_records=gen_summary.get("total_pings", 0),
                artifacts_created=artifacts,
                metrics={
                    "total_devices": total_devices,
                    "duration_hours": duration_hours,
                    "seed": effective_seed,
                    "device_categories": gen_summary.get("device_categories", {}),
                },
            )
            logger.info(
                f"Stage 1 completed: {record.output_records} pings generated across "
                f"{total_devices} devices in {record.duration_seconds}s."
            )
            return record

        except Exception as ex:
            end_ts = datetime.now(timezone.utc)
            duration = (end_ts - start_ts).total_seconds()
            err_msg = str(ex)
            logger.error(f"Stage 1 failed: {err_msg}", exc_info=True)
            return StageExecutionRecord(
                stage_name=stage_name,
                stage_order=1,
                start_time_utc=start_ts.isoformat(),
                end_time_utc=end_ts.isoformat(),
                duration_seconds=round(duration, 3),
                status="FAILED",
                input_records=total_devices,
                output_records=0,
                error_message=err_msg,
            )

    # -------------------------------------------------------------------------
    # STAGE 2: GPS Data Preprocessing and Validation
    # -------------------------------------------------------------------------
    def execute_stage_2_preprocessing(
        self,
        raw_files: Optional[List[Union[str, Path]]] = None,
        output_dir: Optional[Union[str, Path]] = None,
    ) -> StageExecutionRecord:
        """Execute Stage 2: GPS Data Cleaning, Timestamp Normalization & Validation.

        Args:
            raw_files: Optional list of raw telemetry file paths.
            output_dir: Output directory for cleaned datasets.

        Returns:
            StageExecutionRecord detailing cleaning metrics.
        """
        stage_name = PipelineStage.STAGE_2_PREPROCESSING.value
        start_ts = datetime.now(timezone.utc)
        logger.info(f"--- Starting Stage 2: {stage_name} ---")

        target_dir = Path(output_dir) if output_dir else self.processed_dir

        try:
            from src.member1_data_eng.validation.preprocessor import GPSDataPreprocessor

            preprocessor = GPSDataPreprocessor(config=self.config)
            files = [Path(f) for f in raw_files] if raw_files else preprocessor.find_raw_files(self.raw_dir)

            if not files:
                raise FileNotFoundError(f"No raw telemetry files found under {self.raw_dir}.")

            cleaning_summary = preprocessor.process_files(
                raw_files=files,
                output_dir=target_dir,
            )

            end_ts = datetime.now(timezone.utc)
            duration = (end_ts - start_ts).total_seconds()
            artifacts = [
                f
                for f in [
                    cleaning_summary.output_cleaned_file,
                    cleaning_summary.output_rejected_file,
                    cleaning_summary.output_summary_file,
                ]
                if f
            ]

            record = StageExecutionRecord(
                stage_name=stage_name,
                stage_order=2,
                start_time_utc=start_ts.isoformat(),
                end_time_utc=end_ts.isoformat(),
                duration_seconds=round(duration, 3),
                status="SUCCESS",
                input_records=cleaning_summary.total_input_records,
                output_records=cleaning_summary.valid_records,
                artifacts_created=artifacts,
                metrics={
                    "rejected_records": cleaning_summary.rejected_records,
                    "duplicate_records": cleaning_summary.duplicate_records,
                    "invalid_records": cleaning_summary.missing_or_invalid_records,
                    "cleaning_success_percentage": cleaning_summary.cleaning_success_percentage,
                },
            )
            logger.info(
                f"Stage 2 completed: {cleaning_summary.valid_records}/{cleaning_summary.total_input_records} "
                f"records cleaned ({cleaning_summary.cleaning_success_percentage}%) in {record.duration_seconds}s."
            )
            return record

        except Exception as ex:
            end_ts = datetime.now(timezone.utc)
            duration = (end_ts - start_ts).total_seconds()
            err_msg = str(ex)
            logger.error(f"Stage 2 failed: {err_msg}", exc_info=True)
            return StageExecutionRecord(
                stage_name=stage_name,
                stage_order=2,
                start_time_utc=start_ts.isoformat(),
                end_time_utc=end_ts.isoformat(),
                duration_seconds=round(duration, 3),
                status="FAILED",
                input_records=0,
                output_records=0,
                error_message=err_msg,
            )

    # -------------------------------------------------------------------------
    # STAGE 3: Store Catchment Generation
    # -------------------------------------------------------------------------
    def execute_stage_3_catchments(
        self,
        buffer_radius_meters: float = 500.0,
        spatial_dir: Optional[Union[str, Path]] = None,
    ) -> StageExecutionRecord:
        """Execute Stage 3: Store Catchment Polygon Generation.

        Args:
            buffer_radius_meters: Circular catchment buffer radius in meters.
            spatial_dir: Target directory for GeoJSON files.

        Returns:
            StageExecutionRecord detailing catchment generation metrics.
        """
        stage_name = PipelineStage.STAGE_3_CATCHMENTS.value
        start_ts = datetime.now(timezone.utc)
        logger.info(f"--- Starting Stage 3: {stage_name} ---")

        target_dir = Path(spatial_dir) if spatial_dir else self.spatial_dir

        try:
            from src.member1_data_eng.spatial_processing.catchment_generator import (
                StoreCatchmentGenerator,
            )

            generator = StoreCatchmentGenerator(config=self.config)
            catchment_summary = generator.run_pipeline(
                buffer_radius_meters=buffer_radius_meters,
                spatial_dir=target_dir,
            )

            end_ts = datetime.now(timezone.utc)
            duration = (end_ts - start_ts).total_seconds()
            artifacts = list(catchment_summary.output_files.values())
            mean_area = (
                round(
                    sum(s.get("area_sq_meters", 0.0) for s in catchment_summary.stores)
                    / len(catchment_summary.stores),
                    2,
                )
                if catchment_summary.stores
                else 0.0
            )

            record = StageExecutionRecord(
                stage_name=stage_name,
                stage_order=3,
                start_time_utc=start_ts.isoformat(),
                end_time_utc=end_ts.isoformat(),
                duration_seconds=round(duration, 3),
                status="SUCCESS",
                input_records=catchment_summary.total_stores,
                output_records=catchment_summary.processed_stores,
                artifacts_created=artifacts,
                metrics={
                    "buffer_radius_meters": catchment_summary.buffer_radius_meters,
                    "projected_crs": catchment_summary.projected_crs,
                    "mean_catchment_area_sqm": mean_area,
                },
            )
            logger.info(
                f"Stage 3 completed: {catchment_summary.processed_stores} store catchments "
                f"generated ({buffer_radius_meters}m) in {record.duration_seconds}s."
            )
            return record

        except Exception as ex:
            end_ts = datetime.now(timezone.utc)
            duration = (end_ts - start_ts).total_seconds()
            err_msg = str(ex)
            logger.error(f"Stage 3 failed: {err_msg}", exc_info=True)
            return StageExecutionRecord(
                stage_name=stage_name,
                stage_order=3,
                start_time_utc=start_ts.isoformat(),
                end_time_utc=end_ts.isoformat(),
                duration_seconds=round(duration, 3),
                status="FAILED",
                input_records=0,
                output_records=0,
                error_message=err_msg,
            )

    # -------------------------------------------------------------------------
    # STAGE 4: Spatial Join Processing
    # -------------------------------------------------------------------------
    def execute_stage_4_spatial_join(
        self,
        pings_path: Optional[Union[str, Path, Any]] = None,
        catchments_path: Optional[Union[str, Path, Any]] = None,
    ) -> Tuple[StageExecutionRecord, Any]:
        """Execute Stage 4: Point-in-Polygon Spatial Join.

        Args:
            pings_path: Path to cleaned GPS data CSV, or DataFrame.
            catchments_path: Path to store catchments GeoJSON, or GeoDataFrame.

        Returns:
            Tuple of (StageExecutionRecord, joined_df).
        """
        stage_name = PipelineStage.STAGE_4_SPATIAL_JOIN.value
        start_ts = datetime.now(timezone.utc)
        logger.info(f"--- Starting Stage 4: {stage_name} ---")

        pings_src = Path(pings_path) if isinstance(pings_path, (str, Path)) else (
            pings_path or (self.processed_dir / "cleaned_gps_data.csv")
        )
        catchments_src = Path(catchments_path) if isinstance(catchments_path, (str, Path)) else (
            catchments_path or (self.spatial_dir / "store_catchments_500m.geojson")
        )

        try:
            from src.member1_data_eng.spatial_processing.sedona_spatial_joins import (
                SedonaSpatialJoinEngine,
            )

            engine = SedonaSpatialJoinEngine(config=self.config)
            joined_df = engine.join_pings_with_catchments(
                pings_data=pings_src,
                catchments_data=catchments_src,
            )

            # Determine input record count
            if hasattr(pings_src, "__len__"):
                input_count = len(pings_src)
            elif isinstance(pings_src, Path) and pings_src.exists():
                input_count = len(pd.read_csv(pings_src))
            else:
                input_count = len(joined_df)

            output_count = len(joined_df)
            end_ts = datetime.now(timezone.utc)
            duration = (end_ts - start_ts).total_seconds()

            matched_stores = int(joined_df["store_id"].nunique()) if output_count > 0 else 0
            matched_devices = int(joined_df["device_id"].nunique()) if output_count > 0 else 0

            record = StageExecutionRecord(
                stage_name=stage_name,
                stage_order=4,
                start_time_utc=start_ts.isoformat(),
                end_time_utc=end_ts.isoformat(),
                duration_seconds=round(duration, 3),
                status="SUCCESS",
                input_records=input_count,
                output_records=output_count,
                metrics={
                    "matched_stores": matched_stores,
                    "matched_devices": matched_devices,
                    "spatial_match_percentage": round(
                        (output_count / input_count * 100.0) if input_count > 0 else 0.0, 2
                    ),
                },
            )
            logger.info(
                f"Stage 4 completed: {output_count}/{input_count} pings spatially matched "
                f"inside catchments ({record.metrics['spatial_match_percentage']}%) in {record.duration_seconds}s."
            )
            return record, joined_df

        except Exception as ex:
            end_ts = datetime.now(timezone.utc)
            duration = (end_ts - start_ts).total_seconds()
            err_msg = str(ex)
            logger.error(f"Stage 4 failed: {err_msg}", exc_info=True)
            record = StageExecutionRecord(
                stage_name=stage_name,
                stage_order=4,
                start_time_utc=start_ts.isoformat(),
                end_time_utc=end_ts.isoformat(),
                duration_seconds=round(duration, 3),
                status="FAILED",
                input_records=0,
                output_records=0,
                error_message=err_msg,
            )
            return record, pd.DataFrame() if PANDAS_AVAILABLE else None

    # -------------------------------------------------------------------------
    # STAGE 5: Store Visit Detection
    # -------------------------------------------------------------------------
    def execute_stage_5_visit_detection(
        self,
        joined_df: Optional[Any] = None,
        output_dir: Optional[Union[str, Path]] = None,
    ) -> StageExecutionRecord:
        """Execute Stage 5: Store Visit & Catchment Presence Event Detection.

        Args:
            joined_df: Optional spatially joined pings DataFrame from Stage 4.
            output_dir: Output directory for event files.

        Returns:
            StageExecutionRecord detailing event detection metrics.
        """
        stage_name = PipelineStage.STAGE_5_VISIT_DETECTION.value
        start_ts = datetime.now(timezone.utc)
        logger.info(f"--- Starting Stage 5: {stage_name} ---")

        target_dir = Path(output_dir) if output_dir else self.processed_dir

        try:
            from src.member1_data_eng.spatial_processing.visit_detector import StoreVisitDetector

            detector = StoreVisitDetector(config=self.config)

            if joined_df is not None and len(joined_df) > 0:
                # Direct detection from in-memory joined pings
                events = detector.detect_continuous_events(joined_df)
                events_df = detector.events_to_dataframe(events)

                # Validation
                from src.member1_data_eng.spatial_processing.visit_detector import (
                    VisitDetectionSummary,
                    validate_visit_events,
                )

                events_valid, events_errs = validate_visit_events(events)
                if not events_valid:
                    raise ValueError(f"Visit events validation failed: {events_errs}")

                confirmed_count = sum(1 for e in events if e.is_confirmed_visit)
                presence_count = len(events) - confirmed_count
                unique_devices = len({e.device_id for e in events})

                store_visit_counts: Dict[str, int] = {}
                for e in events:
                    store_visit_counts[e.store_id] = store_visit_counts.get(e.store_id, 0) + 1

                dwell_minutes_list = [e.dwell_duration_minutes for e in events]
                dwell_stats = {
                    "min_minutes": round(min(dwell_minutes_list), 2) if dwell_minutes_list else 0.0,
                    "max_minutes": round(max(dwell_minutes_list), 2) if dwell_minutes_list else 0.0,
                    "mean_minutes": round(sum(dwell_minutes_list) / len(dwell_minutes_list), 2)
                    if dwell_minutes_list
                    else 0.0,
                }

                summary = VisitDetectionSummary(
                    total_input_pings=len(joined_df),
                    pings_in_catchments=len(joined_df),
                    total_events_detected=len(events),
                    confirmed_store_visits=confirmed_count,
                    catchment_presence_events=presence_count,
                    unique_devices_detected=unique_devices,
                    stores_visited=store_visit_counts,
                    dwell_time_summary_minutes=dwell_stats,
                )
                written_files = detector.save_outputs(
                    events_df=events_df, summary=summary, output_dir=target_dir
                )
                artifacts = list(written_files.values())
                input_count = len(joined_df)
                output_count = len(events)

            else:
                # File-driven execution
                pings_file = self.processed_dir / "cleaned_gps_data.csv"
                catchments_file = self.spatial_dir / "store_catchments_500m.geojson"
                detection_summary = detector.run_detection_pipeline(
                    pings_data=pings_file,
                    catchments_data=catchments_file,
                    output_dir=target_dir,
                )
                artifacts = list(detection_summary.output_files.values())
                input_count = detection_summary.pings_in_catchments
                output_count = detection_summary.total_events_detected
                confirmed_count = detection_summary.confirmed_store_visits
                presence_count = detection_summary.catchment_presence_events
                unique_devices = detection_summary.unique_devices_detected
                dwell_stats = detection_summary.dwell_time_summary_minutes

            end_ts = datetime.now(timezone.utc)
            duration = (end_ts - start_ts).total_seconds()

            record = StageExecutionRecord(
                stage_name=stage_name,
                stage_order=5,
                start_time_utc=start_ts.isoformat(),
                end_time_utc=end_ts.isoformat(),
                duration_seconds=round(duration, 3),
                status="SUCCESS",
                input_records=input_count,
                output_records=output_count,
                artifacts_created=artifacts,
                metrics={
                    "confirmed_visits": confirmed_count,
                    "catchment_presence_events": presence_count,
                    "unique_devices": unique_devices,
                    "mean_dwell_minutes": dwell_stats.get("mean_minutes", 0.0),
                },
            )
            logger.info(
                f"Stage 5 completed: {output_count} events detected ({confirmed_count} confirmed visits, "
                f"{presence_count} transits) across {unique_devices} devices in {record.duration_seconds}s."
            )
            return record

        except Exception as ex:
            end_ts = datetime.now(timezone.utc)
            duration = (end_ts - start_ts).total_seconds()
            err_msg = str(ex)
            logger.error(f"Stage 5 failed: {err_msg}", exc_info=True)
            return StageExecutionRecord(
                stage_name=stage_name,
                stage_order=5,
                start_time_utc=start_ts.isoformat(),
                end_time_utc=end_ts.isoformat(),
                duration_seconds=round(duration, 3),
                status="FAILED",
                input_records=0,
                output_records=0,
                error_message=err_msg,
            )

    # -------------------------------------------------------------------------
    # STAGE 6: Store Visit Analytics
    # -------------------------------------------------------------------------
    def execute_stage_6_analytics(
        self,
        events_path: Optional[Union[str, Path, Any]] = None,
        output_dir: Optional[Union[str, Path]] = None,
    ) -> StageExecutionRecord:
        """Execute Stage 6: Store-Level, Device-Level & Hourly Mobility Analytics.

        Args:
            events_path: Path to events CSV / Parquet, or DataFrame.
            output_dir: Output analytics directory.

        Returns:
            StageExecutionRecord detailing analytics metrics.
        """
        stage_name = PipelineStage.STAGE_6_ANALYTICS.value
        start_ts = datetime.now(timezone.utc)
        logger.info(f"--- Starting Stage 6: {stage_name} ---")

        events_src = events_path or (self.processed_dir / "store_catchment_events.csv")
        target_dir = Path(output_dir) if output_dir else self.analytics_dir

        try:
            from src.member1_data_eng.spatial_processing.mobility_metrics import (
                run_analytics_pipeline,
            )

            analytics_summary = run_analytics_pipeline(
                events_path=events_src,
                output_dir=target_dir,
            )

            end_ts = datetime.now(timezone.utc)
            duration = (end_ts - start_ts).total_seconds()
            artifacts = list(analytics_summary.get("output_files", {}).values())

            overall = analytics_summary.get("overall_summary", {})
            input_count = analytics_summary.get("analytics_metadata", {}).get("total_input_events", 0)
            output_count = (
                overall.get("unique_stores", 0)
                + overall.get("unique_devices", 0)
            )

            record = StageExecutionRecord(
                stage_name=stage_name,
                stage_order=6,
                start_time_utc=start_ts.isoformat(),
                end_time_utc=end_ts.isoformat(),
                duration_seconds=round(duration, 3),
                status="SUCCESS",
                input_records=input_count,
                output_records=output_count,
                artifacts_created=artifacts,
                metrics={
                    "total_detected_visits": overall.get("total_detected_visits", 0),
                    "unique_devices": overall.get("unique_devices", 0),
                    "unique_stores": overall.get("unique_stores", 0),
                    "peak_visiting_hours": analytics_summary.get("hourly_insights", {}).get(
                        "peak_visiting_hours_utc", []
                    ),
                    "avg_dwell_minutes": overall.get("dwell_time_summary_minutes", {}).get(
                        "mean_minutes", 0.0
                    ),
                },
            )
            logger.info(
                f"Stage 6 completed: Analyzed {input_count} events across "
                f"{overall.get('unique_stores', 0)} stores and {overall.get('unique_devices', 0)} "
                f"devices in {record.duration_seconds}s."
            )
            return record

        except Exception as ex:
            end_ts = datetime.now(timezone.utc)
            duration = (end_ts - start_ts).total_seconds()
            err_msg = str(ex)
            logger.error(f"Stage 6 failed: {err_msg}", exc_info=True)
            return StageExecutionRecord(
                stage_name=stage_name,
                stage_order=6,
                start_time_utc=start_ts.isoformat(),
                end_time_utc=end_ts.isoformat(),
                duration_seconds=round(duration, 3),
                status="FAILED",
                input_records=0,
                output_records=0,
                error_message=err_msg,
            )

    # -------------------------------------------------------------------------
    # STAGE: Cannibalization & Visitor Overlap Analysis
    # -------------------------------------------------------------------------
    def execute_cannibalization_analysis(
        self,
        events_path: Optional[Union[str, Path, Any]] = None,
        catchments_path: Optional[Union[str, Path, Any]] = None,
        output_dir: Optional[Union[str, Path]] = None,
    ) -> StageExecutionRecord:
        """Execute pairwise cannibalization and visitor overlap analysis.

        Args:
            events_path: Path to events CSV, or DataFrame.
            catchments_path: Path to catchments GeoJSON, or GeoDataFrame.
            output_dir: Output analytics directory.

        Returns:
            StageExecutionRecord detailing execution metrics.
        """
        stage_name = "cannibalization_analysis"
        start_ts = datetime.now(timezone.utc)
        logger.info(f"--- Starting Stage: {stage_name} ---")

        events_src = events_path or (self.processed_dir / "store_catchment_events.csv")
        catchments_src = catchments_path or (self.spatial_dir / "store_catchments_500m.geojson")
        target_dir = Path(output_dir) if output_dir else self.analytics_dir

        try:
            from src.member1_data_eng.spatial_processing.cannibalization_analyzer import (
                CannibalizationAnalyzer,
            )

            analyzer = CannibalizationAnalyzer(config=self.config)
            summary = analyzer.run_analysis_pipeline(
                events_path=events_src,
                catchments_path=catchments_src,
                output_dir=target_dir,
            )

            end_ts = datetime.now(timezone.utc)
            duration = (end_ts - start_ts).total_seconds()
            artifacts = list(summary.output_files.values())

            record = StageExecutionRecord(
                stage_name=stage_name,
                stage_order=7,
                start_time_utc=start_ts.isoformat(),
                end_time_utc=end_ts.isoformat(),
                duration_seconds=round(duration, 3),
                status="SUCCESS",
                input_records=summary.analysis_metadata.get("total_store_pairs", 10),
                output_records=summary.analysis_metadata.get("total_store_pairs", 10),
                artifacts_created=artifacts,
                metrics={
                    "pairs_with_catchment_overlap": summary.overall_summary.get(
                        "pairs_with_catchment_overlap", 0
                    ),
                    "pairs_with_shared_visitors": summary.overall_summary.get(
                        "pairs_with_shared_visitors", 0
                    ),
                    "risk_distribution": summary.risk_category_distribution,
                },
            )
            logger.info(
                f"Cannibalization analysis completed in {record.duration_seconds}s."
            )
            return record

        except Exception as ex:
            end_ts = datetime.now(timezone.utc)
            duration = (end_ts - start_ts).total_seconds()
            err_msg = str(ex)
            logger.error(f"Cannibalization analysis failed: {err_msg}", exc_info=True)
            return StageExecutionRecord(
                stage_name=stage_name,
                stage_order=7,
                start_time_utc=start_ts.isoformat(),
                end_time_utc=end_ts.isoformat(),
                duration_seconds=round(duration, 3),
                status="FAILED",
                input_records=0,
                output_records=0,
                error_message=err_msg,
            )

    # -------------------------------------------------------------------------
    # STAGE 7: Data Quality Monitoring & Pipeline Summary
    # -------------------------------------------------------------------------
    def run_data_quality_audit(self) -> Dict[str, Any]:
        """Perform comprehensive data quality checks across all pipeline deliverables.

        Checks:
        1. Required file integrity and non-empty outputs.
        2. Schema and column presence across tables.
        3. Null identifier checks.
        4. Duplicate ID checks.
        5. Timestamp format and chronological ordering.
        6. Coordinate bounds within configured study zone.
        7. Dwell duration non-negativity.
        8. Cross-stage record-count consistency.

        Returns:
            Dictionary containing overall audit status, score, and list of check results.
        """
        checks: List[QualityCheckResult] = []

        # ---------------------------------------------------------------------
        # Check 1: File Existence & Non-Empty Outputs
        # ---------------------------------------------------------------------
        expected_files = {
            "cleaned_gps_csv": self.processed_dir / "cleaned_gps_data.csv",
            "store_locations_geojson": self.spatial_dir / "store_locations.geojson",
            "store_catchments_geojson": self.spatial_dir / "store_catchments_500m.geojson",
            "catchment_events_csv": self.processed_dir / "store_catchment_events.csv",
            "catchment_events_parquet": self.processed_dir / "store_catchment_events.parquet",
            "store_analytics_csv": self.analytics_dir / "store_visit_analytics.csv",
            "device_analytics_csv": self.analytics_dir / "device_mobility_analytics.csv",
            "hourly_analytics_csv": self.analytics_dir / "hourly_visit_analytics.csv",
            "mobility_summary_json": self.analytics_dir / "mobility_analytics_summary.json",
        }

        missing_files = []
        empty_files = []
        for name, path in expected_files.items():
            if not path.exists():
                missing_files.append(name)
            elif path.stat().st_size == 0:
                empty_files.append(name)

        if missing_files:
            checks.append(QualityCheckResult(
                check_name="CHECK_FILE_EXISTENCE",
                category="FILE_INTEGRITY",
                status="FAILED",
                details=f"Missing required deliverable file(s): {', '.join(missing_files)}.",
                records_checked=len(expected_files),
                records_failed=len(missing_files),
            ))
        else:
            checks.append(QualityCheckResult(
                check_name="CHECK_FILE_EXISTENCE",
                category="FILE_INTEGRITY",
                status="PASSED",
                details="All 9 core pipeline deliverable files exist on disk.",
                records_checked=len(expected_files),
                records_failed=0,
            ))

        if empty_files:
            checks.append(QualityCheckResult(
                check_name="CHECK_EMPTY_OUTPUTS",
                category="FILE_INTEGRITY",
                status="FAILED",
                details=f"Zero-byte output file(s) detected: {', '.join(empty_files)}.",
                records_checked=len(expected_files),
                records_failed=len(empty_files),
            ))
        else:
            checks.append(QualityCheckResult(
                check_name="CHECK_EMPTY_OUTPUTS",
                category="FILE_INTEGRITY",
                status="PASSED",
                details="All deliverable files contain valid non-zero data payload.",
                records_checked=len(expected_files),
                records_failed=0,
            ))

        # Stop deeper content inspection if primary files are missing
        cleaned_csv_path = expected_files["cleaned_gps_csv"]
        events_csv_path = expected_files["catchment_events_csv"]
        store_csv_path = expected_files["store_analytics_csv"]
        device_csv_path = expected_files["device_analytics_csv"]
        hourly_csv_path = expected_files["hourly_analytics_csv"]

        if not cleaned_csv_path.exists() or not events_csv_path.exists():
            total_checks = len(checks)
            passed = sum(1 for c in checks if c.status == "PASSED")
            return {
                "total_checks": total_checks,
                "passed_checks": passed,
                "failed_checks": total_checks - passed,
                "warnings": 0,
                "quality_score_percentage": round(passed / total_checks * 100.0, 2),
                "checks": [c.to_dict() for c in checks],
            }

        # Load DataFrames for deeper checks
        df_cleaned = pd.read_csv(cleaned_csv_path) if PANDAS_AVAILABLE else None
        df_events = pd.read_csv(events_csv_path) if PANDAS_AVAILABLE else None
        df_store = pd.read_csv(store_csv_path) if PANDAS_AVAILABLE and store_csv_path.exists() else None
        df_device = pd.read_csv(device_csv_path) if PANDAS_AVAILABLE and device_csv_path.exists() else None
        df_hourly = pd.read_csv(hourly_csv_path) if PANDAS_AVAILABLE and hourly_csv_path.exists() else None

        # ---------------------------------------------------------------------
        # Check 2: Schema & Required Columns
        # ---------------------------------------------------------------------
        schema_errors = []
        if df_cleaned is not None:
            req_clean = ["ping_id", "device_id", "timestamp_utc", "latitude", "longitude", "speed_mps"]
            missing_clean = [c for c in req_clean if c not in df_cleaned.columns]
            if missing_clean:
                schema_errors.append(f"cleaned_gps missing {missing_clean}")

        if df_events is not None:
            req_evt = ["event_id", "device_id", "store_id", "entry_timestamp_utc", "exit_timestamp_utc", "dwell_duration_minutes"]
            missing_evt = [c for c in req_evt if c not in df_events.columns]
            if missing_evt:
                schema_errors.append(f"events missing {missing_evt}")

        if schema_errors:
            checks.append(QualityCheckResult(
                check_name="CHECK_SCHEMA_COLUMNS",
                category="SCHEMA",
                status="FAILED",
                details=f"Schema violations: {'; '.join(schema_errors)}.",
            ))
        else:
            checks.append(QualityCheckResult(
                check_name="CHECK_SCHEMA_COLUMNS",
                category="SCHEMA",
                status="PASSED",
                details="All tables strictly adhere to mandated column schemas.",
            ))

        # ---------------------------------------------------------------------
        # Check 3: Null Identifiers
        # ---------------------------------------------------------------------
        null_counts = {}
        if df_cleaned is not None:
            null_counts["clean.ping_id"] = int(df_cleaned["ping_id"].isnull().sum())
            null_counts["clean.device_id"] = int(df_cleaned["device_id"].isnull().sum())
        if df_events is not None:
            null_counts["events.event_id"] = int(df_events["event_id"].isnull().sum())
            null_counts["events.store_id"] = int(df_events["store_id"].isnull().sum())
            null_counts["events.device_id"] = int(df_events["device_id"].isnull().sum())

        total_nulls = sum(null_counts.values())
        if total_nulls > 0:
            checks.append(QualityCheckResult(
                check_name="CHECK_NULL_IDENTIFIERS",
                category="NULLS",
                status="FAILED",
                details=f"Found {total_nulls} null identifier(s): {null_counts}.",
                records_checked=len(null_counts),
                records_failed=total_nulls,
            ))
        else:
            checks.append(QualityCheckResult(
                check_name="CHECK_NULL_IDENTIFIERS",
                category="NULLS",
                status="PASSED",
                details="0 null values detected across all mandatory identifier columns.",
                records_checked=len(null_counts),
                records_failed=0,
            ))

        # ---------------------------------------------------------------------
        # Check 4: Duplicate Identifiers
        # ---------------------------------------------------------------------
        dup_errors = []
        if df_cleaned is not None:
            dup_pings = int(df_cleaned["ping_id"].duplicated().sum())
            if dup_pings > 0:
                dup_errors.append(f"{dup_pings} duplicate ping_ids in cleaned_gps")

        if df_events is not None:
            dup_evts = int(df_events["event_id"].duplicated().sum())
            if dup_evts > 0:
                dup_errors.append(f"{dup_evts} duplicate event_ids in events")

        if dup_errors:
            checks.append(QualityCheckResult(
                check_name="CHECK_DUPLICATE_IDS",
                category="DUPLICATES",
                status="FAILED",
                details=f"Duplicate violations: {'; '.join(dup_errors)}.",
            ))
        else:
            checks.append(QualityCheckResult(
                check_name="CHECK_DUPLICATE_IDS",
                category="DUPLICATES",
                status="PASSED",
                details="0 duplicate primary identifiers detected across datasets.",
            ))

        # ---------------------------------------------------------------------
        # Check 5: Timestamp Validity & Chronological Ordering
        # ---------------------------------------------------------------------
        ts_errors = []
        if df_events is not None and len(df_events) > 0:
            entry_ts = pd.to_datetime(df_events["entry_timestamp_utc"], utc=True, errors="coerce")
            exit_ts = pd.to_datetime(df_events["exit_timestamp_utc"], utc=True, errors="coerce")
            unparseable = int(entry_ts.isnull().sum() + exit_ts.isnull().sum())
            if unparseable > 0:
                ts_errors.append(f"{unparseable} unparseable ISO-8601 timestamps")
            inversions = int((exit_ts < entry_ts).sum())
            if inversions > 0:
                ts_errors.append(f"{inversions} records where exit precedes entry")

        if ts_errors:
            checks.append(QualityCheckResult(
                check_name="CHECK_TIMESTAMP_VALIDITY",
                category="LOGIC",
                status="FAILED",
                details=f"Timestamp violations: {'; '.join(ts_errors)}.",
            ))
        else:
            checks.append(QualityCheckResult(
                check_name="CHECK_TIMESTAMP_VALIDITY",
                category="LOGIC",
                status="PASSED",
                details="100% of event timestamps are valid UTC ISO-8601 with chronological order.",
            ))

        # ---------------------------------------------------------------------
        # Check 6: Coordinate Bounds within Study Zone
        # ---------------------------------------------------------------------
        coord_errors = []
        if df_cleaned is not None and len(df_cleaned) > 0:
            bbox = self.config.get("study_zone", {}).get("bounding_box", {})
            min_lat = float(bbox.get("min_lat", 12.9200))
            max_lat = float(bbox.get("max_lat", 12.9450))
            min_lon = float(bbox.get("min_lon", 77.6100))
            max_lon = float(bbox.get("max_lon", 77.6400))

            out_lat = int(((df_cleaned["latitude"] < min_lat) | (df_cleaned["latitude"] > max_lat)).sum())
            out_lon = int(((df_cleaned["longitude"] < min_lon) | (df_cleaned["longitude"] > max_lon)).sum())
            if out_lat > 0 or out_lon > 0:
                coord_errors.append(f"{out_lat} lat and {out_lon} lon points outside study zone bounds")

        if coord_errors:
            checks.append(QualityCheckResult(
                check_name="CHECK_COORDINATE_BOUNDS",
                category="BOUNDS",
                status="FAILED",
                details=f"Spatial boundary violations: {'; '.join(coord_errors)}.",
            ))
        else:
            checks.append(QualityCheckResult(
                check_name="CHECK_COORDINATE_BOUNDS",
                category="BOUNDS",
                status="PASSED",
                details="100% of cleaned GPS telemetry coordinates lie strictly within study zone bounds.",
            ))

        # ---------------------------------------------------------------------
        # Check 7: Dwell Duration Non-Negativity
        # ---------------------------------------------------------------------
        dwell_errors = []
        if df_events is not None and len(df_events) > 0:
            dwells = pd.to_numeric(df_events["dwell_duration_minutes"], errors="coerce")
            neg_dwells = int((dwells < 0).sum())
            if neg_dwells > 0:
                dwell_errors.append(f"{neg_dwells} negative dwell duration records")

        if dwell_errors:
            checks.append(QualityCheckResult(
                check_name="CHECK_NON_NEGATIVE_DWELL",
                category="LOGIC",
                status="FAILED",
                details=f"Dwell violations: {'; '.join(dwell_errors)}.",
            ))
        else:
            checks.append(QualityCheckResult(
                check_name="CHECK_NON_NEGATIVE_DWELL",
                category="LOGIC",
                status="PASSED",
                details="All dwell durations are strictly non-negative (>= 0 minutes).",
            ))

        # ---------------------------------------------------------------------
        # Check 8: Cross-Stage Record Count Consistency
        # ---------------------------------------------------------------------
        consistency_errors = []
        if df_events is not None and df_store is not None and df_device is not None:
            total_events = len(df_events)
            store_visit_sum = int(df_store["total_detected_visits"].sum()) if len(df_store) > 0 else 0
            device_visit_sum = int(df_device["total_detected_visits"].sum()) if len(df_device) > 0 else 0

            if store_visit_sum != total_events:
                consistency_errors.append(
                    f"Store analytics visit sum ({store_visit_sum}) != total events ({total_events})"
                )
            if device_visit_sum != total_events:
                consistency_errors.append(
                    f"Device analytics visit sum ({device_visit_sum}) != total events ({total_events})"
                )

        if consistency_errors:
            checks.append(QualityCheckResult(
                check_name="CHECK_RECORD_COUNT_CONSISTENCY",
                category="CONSISTENCY",
                status="FAILED",
                details=f"Inconsistencies detected: {'; '.join(consistency_errors)}.",
            ))
        else:
            checks.append(QualityCheckResult(
                check_name="CHECK_RECORD_COUNT_CONSISTENCY",
                category="CONSISTENCY",
                status="PASSED",
                details="Complete cross-table volume parity across events, store, and device tables.",
            ))

        total_checks = len(checks)
        passed = sum(1 for c in checks if c.status == "PASSED")
        score = round(passed / total_checks * 100.0, 2) if total_checks > 0 else 0.0

        return {
            "total_checks": total_checks,
            "passed_checks": passed,
            "failed_checks": total_checks - passed,
            "warnings": 0,
            "quality_score_percentage": score,
            "checks": [c.to_dict() for c in checks],
        }

    # -------------------------------------------------------------------------
    # STAGE 8 Execution: Quality Audit & Pipeline Summary File
    # -------------------------------------------------------------------------
    def execute_stage_7_quality_audit(
        self,
        stage_records: Optional[List[StageExecutionRecord]] = None,
        output_dir: Optional[Union[str, Path]] = None,
    ) -> StageExecutionRecord:
        """Execute Stage 7: Quality Audit & Final Pipeline Summary Persistence.

        Args:
            stage_records: List of execution records from prior stages.
            output_dir: Output directory for summary JSON.

        Returns:
            StageExecutionRecord detailing audit metrics.
        """
        stage_name = PipelineStage.STAGE_7_QUALITY_AUDIT.value
        start_ts = datetime.now(timezone.utc)
        logger.info(f"--- Starting Stage 7: {stage_name} ---")

        target_dir = Path(output_dir) if output_dir else self.processed_dir

        try:
            audit_report = self.run_data_quality_audit()
            end_ts = datetime.now(timezone.utc)
            duration = (end_ts - start_ts).total_seconds()

            summary_file = target_dir / "pipeline_execution_summary.json"
            artifacts = [str(summary_file)]

            record = StageExecutionRecord(
                stage_name=stage_name,
                stage_order=8,
                start_time_utc=start_ts.isoformat(),
                end_time_utc=end_ts.isoformat(),
                duration_seconds=round(duration, 3),
                status="SUCCESS" if audit_report["failed_checks"] == 0 else "WARNING",
                input_records=audit_report["total_checks"],
                output_records=audit_report["passed_checks"],
                artifacts_created=artifacts,
                metrics={
                    "total_checks": audit_report["total_checks"],
                    "passed_checks": audit_report["passed_checks"],
                    "failed_checks": audit_report["failed_checks"],
                    "quality_score_percentage": audit_report["quality_score_percentage"],
                },
            )
            logger.info(
                f"Stage 7 completed: {audit_report['passed_checks']}/{audit_report['total_checks']} "
                f"quality checks passed ({audit_report['quality_score_percentage']}%) in {record.duration_seconds}s."
            )
            return record

        except Exception as ex:
            end_ts = datetime.now(timezone.utc)
            duration = (end_ts - start_ts).total_seconds()
            err_msg = str(ex)
            logger.error(f"Stage 7 failed: {err_msg}", exc_info=True)
            return StageExecutionRecord(
                stage_name=stage_name,
                stage_order=8,
                start_time_utc=start_ts.isoformat(),
                end_time_utc=end_ts.isoformat(),
                duration_seconds=round(duration, 3),
                status="FAILED",
                input_records=0,
                output_records=0,
                error_message=err_msg,
            )

    # -------------------------------------------------------------------------
    # Unified End-to-End Execution Pipeline
    # -------------------------------------------------------------------------
    def run_pipeline(
        self,
        run_generation: bool = False,
        total_devices: int = 5,
        duration_hours: float = 0.5,
        date_str: str = "2026-11-17",
        buffer_radius_meters: float = 500.0,
        seed: Optional[int] = None,
        fail_fast: bool = True,
    ) -> PipelineExecutionSummary:
        """Run the complete end-to-end GeoPulse data engineering pipeline.

        Connects:
        1. (Optional) GPS Data Generation
        2. GPS Data Preprocessing and Validation
        3. Store Catchment Generation
        4. Spatial Join Processing
        5. Store Visit Detection
        6. Store Visit Analytics
        7. Final Quality Audit & Pipeline Summary Persistence

        Args:
            run_generation: If True, executes Stage 1 synthetic generation. If False,
                           uses existing raw data files under raw_dir.
            total_devices: Number of mobile devices to simulate if generating.
            duration_hours: Duration in hours if generating.
            date_str: Target date string for partition.
            buffer_radius_meters: Catchment buffer radius in meters (default 500.0).
            seed: Deterministic random seed override.
            fail_fast: If True, halts on first stage failure.

        Returns:
            PipelineExecutionSummary detailing execution results across all stages.
        """
        pipeline_start = datetime.now(timezone.utc)
        exec_id = f"PIPE_EXEC_{pipeline_start.strftime('%Y%m%d_%H%M%S')}"

        logger.info("=" * 70)
        logger.info(f" Starting GeoPulse End-to-End Pipeline [{exec_id}]")
        logger.info("=" * 70)

        stage_records: List[StageExecutionRecord] = []
        effective_seed = seed if seed is not None else self.random_seed

        # ---------------------------------------------------------------------
        # Stage 1: GPS Data Generation (optional / configurable)
        # ---------------------------------------------------------------------
        if run_generation:
            s1 = self.execute_stage_1_generation(
                total_devices=total_devices,
                duration_hours=duration_hours,
                date_str=date_str,
                seed=effective_seed,
            )
            stage_records.append(s1)
            if s1.status == "FAILED" and fail_fast:
                raise PipelineExecutionError(s1.stage_name, s1.error_message or "Unknown error", s1)
        else:
            logger.info("Stage 1 (Generation) bypassed. Utilizing existing raw telemetry files.")

        # ---------------------------------------------------------------------
        # Stage 2: GPS Data Preprocessing & Validation
        # ---------------------------------------------------------------------
        s2 = self.execute_stage_2_preprocessing()
        stage_records.append(s2)
        if s2.status == "FAILED" and fail_fast:
            raise PipelineExecutionError(s2.stage_name, s2.error_message or "Unknown error", s2)

        # ---------------------------------------------------------------------
        # Stage 3: Store Catchment Generation
        # ---------------------------------------------------------------------
        s3 = self.execute_stage_3_catchments(buffer_radius_meters=buffer_radius_meters)
        stage_records.append(s3)
        if s3.status == "FAILED" and fail_fast:
            raise PipelineExecutionError(s3.stage_name, s3.error_message or "Unknown error", s3)

        # ---------------------------------------------------------------------
        # Stage 4: Spatial Join Processing
        # ---------------------------------------------------------------------
        s4, joined_df = self.execute_stage_4_spatial_join()
        stage_records.append(s4)
        if s4.status == "FAILED" and fail_fast:
            raise PipelineExecutionError(s4.stage_name, s4.error_message or "Unknown error", s4)

        # ---------------------------------------------------------------------
        # Stage 5: Store Visit Detection
        # ---------------------------------------------------------------------
        s5 = self.execute_stage_5_visit_detection(joined_df=joined_df)
        stage_records.append(s5)
        if s5.status == "FAILED" and fail_fast:
            raise PipelineExecutionError(s5.stage_name, s5.error_message or "Unknown error", s5)

        # ---------------------------------------------------------------------
        # Stage 6: Store Visit Analytics
        # ---------------------------------------------------------------------
        s6 = self.execute_stage_6_analytics()
        stage_records.append(s6)
        if s6.status == "FAILED" and fail_fast:
            raise PipelineExecutionError(s6.stage_name, s6.error_message or "Unknown error", s6)

        # ---------------------------------------------------------------------
        # Stage 8: Cannibalization & Visitor Overlap Analysis
        # ---------------------------------------------------------------------
        s8 = self.execute_cannibalization_analysis()
        stage_records.append(s8)
        if s8.status == "FAILED" and fail_fast:
            raise PipelineExecutionError(s8.stage_name, s8.error_message or "Unknown error", s8)

        # ---------------------------------------------------------------------
        # Stage 8: Quality Audit & Summary
        # ---------------------------------------------------------------------
        s7 = self.execute_stage_7_quality_audit(stage_records=stage_records)
        stage_records.append(s7)

        # ---------------------------------------------------------------------
        # Build Final Pipeline Summary
        # ---------------------------------------------------------------------
        pipeline_end = datetime.now(timezone.utc)
        total_sec = round((pipeline_end - pipeline_start).total_seconds(), 3)

        succeeded = sum(1 for s in stage_records if s.status in ("SUCCESS", "WARNING"))
        failed = sum(1 for s in stage_records if s.status == "FAILED")
        overall_status = "SUCCESS" if failed == 0 else "FAILED"

        audit_results = self.run_data_quality_audit()

        artifacts_map = {
            "cleaned_gps_csv": str(self.processed_dir / "cleaned_gps_data.csv"),
            "rejected_gps_csv": str(self.processed_dir / "rejected_gps_data.csv"),
            "store_locations_geojson": str(self.spatial_dir / "store_locations.geojson"),
            "store_catchments_geojson": str(self.spatial_dir / "store_catchments_500m.geojson"),
            "catchment_events_csv": str(self.processed_dir / "store_catchment_events.csv"),
            "catchment_events_parquet": str(self.processed_dir / "store_catchment_events.parquet"),
            "store_analytics_csv": str(self.analytics_dir / "store_visit_analytics.csv"),
            "device_analytics_csv": str(self.analytics_dir / "device_mobility_analytics.csv"),
            "hourly_analytics_csv": str(self.analytics_dir / "hourly_visit_analytics.csv"),
            "mobility_summary_json": str(self.analytics_dir / "mobility_analytics_summary.json"),
            "pipeline_summary_json": str(self.processed_dir / "pipeline_execution_summary.json"),
        }

        config_snapshot = {
            "random_seed": effective_seed,
            "study_zone_id": self.config.get("study_zone", {}).get("id", "IND_BLR_KORAMANGALA"),
            "buffer_radius_meters": buffer_radius_meters,
            "min_dwell_minutes": float(
                self.config.get("dwell_time_criteria", {}).get("min_dwell_minutes", 5.0)
            ),
            "run_generation": run_generation,
            "fail_fast": fail_fast,
        }

        summary = PipelineExecutionSummary(
            execution_id=exec_id,
            execution_start_utc=pipeline_start.isoformat(),
            execution_end_utc=pipeline_end.isoformat(),
            total_duration_seconds=total_sec,
            overall_status=overall_status,
            stages_executed=len(stage_records),
            stages_succeeded=succeeded,
            stages_failed=failed,
            stages=[s.to_dict() for s in stage_records],
            data_quality_report=audit_results,
            output_artifacts=artifacts_map,
            pipeline_configuration=config_snapshot,
        )

        # Save pipeline execution summary JSON
        summary_path = self.processed_dir / "pipeline_execution_summary.json"
        with open(summary_path, "w", encoding="utf-8") as f:
            f.write(summary.to_json(indent=2))

        logger.info("=" * 70)
        logger.info(
            f" GeoPulse Pipeline Complete: {overall_status} "
            f"({succeeded}/{len(stage_records)} stages succeeded, "
            f"Quality Score: {audit_results['quality_score_percentage']}%) in {total_sec}s."
        )
        logger.info(f" Pipeline execution summary written to: {summary_path}")
        logger.info("=" * 70)

        return summary


def run_cli() -> None:
    """Command-line interface entry point for GeoPulse pipeline execution."""
    import argparse

    parser = argparse.ArgumentParser(
        description="GeoPulse End-to-End Data Engineering Pipeline Orchestrator"
    )
    parser.add_argument(
        "--run-generation",
        action="store_true",
        default=False,
        help="Run Stage 1 GPS trajectory generator before processing",
    )
    parser.add_argument(
        "--devices",
        type=int,
        default=5,
        help="Device count for Stage 1 generation (default: 5)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=0.5,
        help="Duration in hours for Stage 1 generation (default: 0.5)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Deterministic random seed (default: 42)",
    )
    parser.add_argument(
        "--buffer-radius",
        type=float,
        default=500.0,
        help="Store catchment buffer radius in meters (default: 500.0)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Destination directory for processed outputs",
    )

    args = parser.parse_args()

    orchestrator = GeoPulsePipelineOrchestrator(
        random_seed=args.seed,
        processed_dir=args.output_dir,
    )
    summary = orchestrator.run_pipeline(
        run_generation=args.run_generation,
        total_devices=args.devices,
        duration_hours=args.duration,
        buffer_radius_meters=args.buffer_radius,
        seed=args.seed,
    )

    print("\n" + "=" * 75)
    print(" GeoPulse End-to-End Pipeline Execution Summary")
    print("=" * 75)
    print(f"Execution ID     : {summary.execution_id}")
    print(f"Overall Status   : {summary.overall_status}")
    print(f"Total Duration   : {summary.total_duration_seconds}s")
    print(f"Stages Executed  : {summary.stages_executed} ({summary.stages_succeeded} succeeded)")
    print(f"Quality Score    : {summary.data_quality_report['quality_score_percentage']}%")
    print("=" * 75)


if __name__ == "__main__":
    run_cli()
