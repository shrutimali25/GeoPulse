"""PySpark and distributed spatial analytics package."""

from src.member1_data_eng.spatial_processing.catchment_generator import (
    DEFAULT_BUFFER_RADIUS_METERS,
    DEFAULT_PROJECTED_CRS,
    CatchmentSummary,
    StoreCatchmentGenerator,
    StoreValidationResult,
    validate_store_records,
)
from src.member1_data_eng.spatial_processing.mobility_metrics import MobilityMetricsCalculator
from src.member1_data_eng.spatial_processing.pyspark_processor import (
    GPS_TELEMETRY_SCHEMA_DICT,
    REQUIRED_COLUMNS,
    ProcessingMetrics,
    PySparkGPSProcessor,
)
from src.member1_data_eng.spatial_processing.sedona_spatial_joins import (
    SedonaSpatialJoinEngine,
    create_sedona_spark_session,
)
from src.member1_data_eng.spatial_processing.visit_detector import (
    StoreVisitDetector,
    StoreVisitEvent,
    VisitDetectionSummary,
    validate_input_pings,
    validate_visit_events,
)

__all__ = [
    "PySparkGPSProcessor",
    "ProcessingMetrics",
    "GPS_TELEMETRY_SCHEMA_DICT",
    "REQUIRED_COLUMNS",
    "StoreCatchmentGenerator",
    "CatchmentSummary",
    "StoreValidationResult",
    "validate_store_records",
    "DEFAULT_BUFFER_RADIUS_METERS",
    "DEFAULT_PROJECTED_CRS",
    "SedonaSpatialJoinEngine",
    "create_sedona_spark_session",
    "StoreVisitDetector",
    "StoreVisitEvent",
    "VisitDetectionSummary",
    "MobilityMetricsCalculator",
    "validate_input_pings",
    "validate_visit_events",
]
