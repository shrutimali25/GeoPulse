"""PySpark Distributed Data Processing Pipeline for GeoPulse.

Loads cleaned GPS telemetry into Apache Spark DataFrames, validates schemas,
detects nulls and duplicates, computes mobility metrics, and saves outputs in Parquet format.
"""

from dataclasses import asdict, dataclass, field
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from src.common.config_loader import get_project_root, load_yaml_config
from src.common.logger import get_logger

logger = get_logger(__name__)

# Try importing PySpark
try:
    import pyspark
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.functions import (
        avg,
        col,
        count,
        desc,
        hour,
        max as spark_max,
        min as spark_min,
        round as spark_round,
        to_timestamp,
        when,
    )
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    SparkSession = Any  # type: ignore
    DataFrame = Any  # type: ignore

# Canonical schema definition as dictionary for compatibility and unit tests
GPS_TELEMETRY_SCHEMA_DICT: Dict[str, str] = {
    "ping_id": "string",
    "device_id": "string",
    "timestamp_utc": "string",
    "latitude": "double",
    "longitude": "double",
    "altitude_meters": "double",
    "horizontal_accuracy_meters": "double",
    "speed_mps": "double",
    "heading_degrees": "double",
    "is_synthetic": "boolean",
    "profile_type": "string",
}

# Mandatory non-nullable columns
REQUIRED_COLUMNS: List[str] = [
    "ping_id",
    "device_id",
    "timestamp_utc",
    "latitude",
    "longitude",
    "horizontal_accuracy_meters",
    "speed_mps",
    "is_synthetic",
    "profile_type",
]

# Recommended Java 21 reflection flags for Apache Spark 3.5.x
JAVA_21_JVM_FLAGS: List[str] = [
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
]


@dataclass
class ProcessingMetrics:
    """Calculated metrics from PySpark distributed processing."""

    total_pings: int = 0
    unique_devices: int = 0
    device_ping_counts: Dict[str, int] = field(default_factory=dict)
    hourly_activity: Dict[int, int] = field(default_factory=dict)
    avg_speed_per_device: Dict[str, float] = field(default_factory=dict)
    null_counts: Dict[str, int] = field(default_factory=dict)
    duplicate_pings_count: int = 0
    output_parquet_paths: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        """Convert metrics to JSON formatted string."""
        return json.dumps(self.to_dict(), indent=indent)


class PySparkGPSProcessor:
    """PySpark processing engine for cleaned GPS telemetry.

    Executes schema enforcement, null detection, deduplication,
    mobility aggregations, and Parquet persistence.
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        spark_session: Optional[Any] = None,
    ):
        self.config = config or load_yaml_config()
        self.spark = spark_session

    @classmethod
    def get_explicit_schema(cls) -> Any:
        """Return explicit PySpark StructType schema if PySpark is available."""
        if not PYSPARK_AVAILABLE:
            return GPS_TELEMETRY_SCHEMA_DICT

        return StructType([
            StructField("ping_id", StringType(), False),
            StructField("device_id", StringType(), False),
            StructField("timestamp_utc", StringType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("altitude_meters", DoubleType(), True),
            StructField("horizontal_accuracy_meters", DoubleType(), False),
            StructField("speed_mps", DoubleType(), False),
            StructField("heading_degrees", DoubleType(), True),
            StructField("is_synthetic", BooleanType(), False),
            StructField("profile_type", StringType(), False),
        ])

    def create_spark_session(self) -> Any:
        """Create and configure a local SparkSession with Java 21 compatibility flags."""
        if not PYSPARK_AVAILABLE:
            raise ImportError(
                "PySpark is not installed. Install dependencies via: "
                "pip install pyspark==3.5.1 pyarrow>=15.0.0"
            )

        if self.spark is not None:
            return self.spark

        spark_cfg = self.config.get("spark_sedona", {})
        app_name = spark_cfg.get("app_name", "GeoPulse-PySpark-Processor")
        master = spark_cfg.get("master", "local[*]")
        driver_mem = spark_cfg.get("driver_memory", "2g")

        jvm_options = " ".join(JAVA_21_JVM_FLAGS)

        builder = (
            SparkSession.builder.appName(app_name)
            .master(master)
            .config("spark.driver.memory", driver_mem)
            .config("spark.driver.extraJavaOptions", jvm_options)
            .config("spark.executor.extraJavaOptions", jvm_options)
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        )

        # Set Hadoop home directory if configured
        hadoop_home = os.getenv("HADOOP_HOME")
        if hadoop_home:
            builder = builder.config("spark.hadoop.home.dir", hadoop_home)

        logger.info(f"Initializing SparkSession '{app_name}' with master '{master}'...")
        self.spark = builder.getOrCreate()
        return self.spark

    def load_cleaned_data(self, csv_path: Optional[Union[str, Path]] = None) -> Any:
        """Load cleaned GPS CSV into Spark DataFrame with explicit schema enforcement."""
        spark = self.create_spark_session()

        if csv_path is None:
            csv_path = get_project_root() / "data" / "processed" / "cleaned_gps_data.csv"
        csv_file = Path(csv_path)

        if not csv_file.exists():
            raise FileNotFoundError(f"Cleaned GPS dataset not found at: {csv_file}")

        logger.info(f"Loading cleaned GPS data from: {csv_file}")
        schema = self.get_explicit_schema()

        df = (
            spark.read.format("csv")
            .option("header", "true")
            .schema(schema)
            .load(str(csv_file))
        )
        return df

    def validate_columns_and_types(self, df: Any) -> Tuple[bool, List[str]]:
        """Validate required columns and expected types in the Spark DataFrame."""
        if not PYSPARK_AVAILABLE:
            raise ImportError("PySpark is not installed.")

        errors: List[str] = []
        actual_fields = {f.name: f.dataType.simpleString() for f in df.schema.fields}

        for req_col in REQUIRED_COLUMNS:
            if req_col not in actual_fields:
                errors.append(f"Missing required column: '{req_col}'")
            else:
                expected_type = GPS_TELEMETRY_SCHEMA_DICT[req_col]
                actual_type = actual_fields[req_col]
                if actual_type != expected_type:
                    errors.append(
                        f"Column '{req_col}' type mismatch: expected {expected_type}, got {actual_type}"
                    )

        is_valid = len(errors) == 0
        if not is_valid:
            logger.error(f"Schema validation failed: {errors}")
        else:
            logger.info("Schema validation passed: All required columns and types verified.")

        return is_valid, errors

    def detect_nulls_and_duplicates(self, df: Any) -> Tuple[Dict[str, int], int]:
        """Detect null counts across required columns and count duplicate ping IDs."""
        if not PYSPARK_AVAILABLE:
            raise ImportError("PySpark is not installed.")

        # Check null counts
        null_exprs = [
            count(when(col(c).isNull(), c)).alias(c)
            for c in REQUIRED_COLUMNS
        ]
        null_row = df.select(null_exprs).collect()[0]
        null_counts = {c: int(null_row[c]) for c in REQUIRED_COLUMNS}

        # Check duplicate pings
        total_count = df.count()
        distinct_pings = df.select("ping_id").distinct().count()
        duplicate_pings = max(0, total_count - distinct_pings)

        logger.info(f"Null counts: {null_counts}")
        logger.info(f"Total rows: {total_count:,}, Duplicate ping_ids: {duplicate_pings}")

        return null_counts, duplicate_pings

    def convert_timestamp(self, df: Any) -> Any:
        """Convert string timestamp_utc column to Spark TimestampType."""
        if not PYSPARK_AVAILABLE:
            raise ImportError("PySpark is not installed.")

        df_converted = df.withColumn(
            "timestamp",
            to_timestamp(col("timestamp_utc"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
        )
        return df_converted

    def calculate_metrics(self, df: Any) -> Tuple[Any, ProcessingMetrics]:
        """Perform distributed mobility aggregations:

        - Total GPS pings
        - Unique devices
        - Device-wise ping counts
        - Hourly GPS activity
        - Average speed per device
        """
        if not PYSPARK_AVAILABLE:
            raise ImportError("PySpark is not installed.")

        # 1. Ensure timestamp column is converted
        df_ts = self.convert_timestamp(df)

        # 2. Check nulls and duplicates
        null_counts, dup_count = self.detect_nulls_and_duplicates(df_ts)

        # 3. Overall counts
        total_pings = df_ts.count()
        unique_devices = df_ts.select("device_id").distinct().count()

        # 4. Device-wise ping counts
        device_counts_df = (
            df_ts.groupBy("device_id")
            .agg(count("*").alias("ping_count"))
            .orderBy(desc("ping_count"))
        )
        device_ping_counts = {
            row["device_id"]: int(row["ping_count"])
            for row in device_counts_df.collect()
        }

        # 5. Hourly GPS activity
        hourly_df = (
            df_ts.withColumn("hour", hour(col("timestamp")))
            .groupBy("hour")
            .agg(count("*").alias("ping_count"))
            .orderBy("hour")
        )
        hourly_activity = {
            int(row["hour"]): int(row["ping_count"])
            for row in hourly_df.collect()
            if row["hour"] is not None
        }

        # 6. Average and max speed per device
        speed_df = (
            df_ts.groupBy("device_id")
            .agg(
                spark_round(avg("speed_mps"), 2).alias("avg_speed_mps"),
                spark_round(spark_max("speed_mps"), 2).alias("max_speed_mps"),
            )
            .orderBy("device_id")
        )
        avg_speed_per_device = {
            row["device_id"]: float(row["avg_speed_mps"])
            for row in speed_df.collect()
            if row["avg_speed_mps"] is not None
        }

        metrics = ProcessingMetrics(
            total_pings=total_pings,
            unique_devices=unique_devices,
            device_ping_counts=device_ping_counts,
            hourly_activity=hourly_activity,
            avg_speed_per_device=avg_speed_per_device,
            null_counts=null_counts,
            duplicate_pings_count=dup_count,
        )

        return df_ts, metrics

    def save_parquet_outputs(
        self,
        df: Any,
        metrics: ProcessingMetrics,
        output_dir: Optional[Union[str, Path]] = None,
    ) -> List[str]:
        """Save processed Spark DataFrame and aggregated metrics as Parquet tables."""
        if not PYSPARK_AVAILABLE:
            raise ImportError("PySpark is not installed.")

        if output_dir is None:
            out_path = get_project_root() / "data" / "processed" / "spark_metrics"
        else:
            out_path = Path(output_dir)

        out_path.mkdir(parents=True, exist_ok=True)
        written_paths: List[str] = []

        # 1. Save cleaned GPS telemetry in Parquet format
        cleaned_parquet = out_path / "cleaned_telemetry.parquet"
        logger.info(f"Writing Parquet telemetry to: {cleaned_parquet}")
        df.write.mode("overwrite").parquet(str(cleaned_parquet))
        written_paths.append(str(cleaned_parquet))

        metrics.output_parquet_paths = written_paths

        # 2. Save metrics summary JSON
        summary_path = out_path / "spark_processing_summary.json"
        with open(summary_path, "w", encoding="utf-8") as f:
            f.write(metrics.to_json(indent=2))

        logger.info(f"Saved PySpark processing summary to: {summary_path}")
        return written_paths

    def run_pipeline(
        self,
        csv_path: Optional[Union[str, Path]] = None,
        output_dir: Optional[Union[str, Path]] = None,
    ) -> ProcessingMetrics:
        """Execute end-to-end PySpark processing pipeline."""
        df = self.load_cleaned_data(csv_path)

        is_valid, errors = self.validate_columns_and_types(df)
        if not is_valid:
            raise ValueError(f"Schema validation errors: {errors}")

        df_transformed, metrics = self.calculate_metrics(df)
        self.save_parquet_outputs(df_transformed, metrics, output_dir)

        logger.info(
            f"PySpark processing complete: {metrics.total_pings:,} pings processed across "
            f"{metrics.unique_devices} unique devices."
        )
        return metrics
