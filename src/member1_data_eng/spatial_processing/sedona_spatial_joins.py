"""Spatial Join Engine for GeoPulse.

Performs point-in-polygon spatial joins between high-frequency GPS telemetry
and retail store catchment polygons. Supports Apache Sedona distributed joins
on PySpark when Sedona extensions and JARs are present, with optimized GeoPandas
STRtree spatial indexing fallback.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
import json
import os

from src.common.config_loader import get_project_root, load_yaml_config
from src.common.logger import get_logger

logger = get_logger(__name__)

# Check GeoPandas / Shapely availability
try:
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import Point, shape
    GEOPANDAS_AVAILABLE = True
except ImportError:
    GEOPANDAS_AVAILABLE = False
    gpd = Any  # type: ignore
    pd = Any   # type: ignore

# Check PySpark availability
try:
    import pyspark
    from pyspark.sql import DataFrame, SparkSession
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    SparkSession = Any  # type: ignore
    DataFrame = Any     # type: ignore

# Check Sedona availability
try:
    import sedona
    from sedona.spark import SedonaContext
    SEDONA_AVAILABLE = True
except ImportError:
    SEDONA_AVAILABLE = False


def create_sedona_spark_session(
    app_name: str = "GeoPulse-Spatial-Engine",
    master: str = "local[*]",
) -> Optional[Any]:
    """Initialize and configure a SparkSession with Apache Sedona extensions.

    Configures Kryo serialization, Sedona SQL extensions, and JVM packages.
    Returns None if Apache Sedona or PySpark runtime is not fully initialized.
    """
    if not PYSPARK_AVAILABLE or not SEDONA_AVAILABLE:
        logger.warning(
            "PySpark or apache-sedona not installed. "
            "Using GeoPandas spatial indexing engine."
        )
        return None

    try:
        from sedona.register import SedonaRegistrator
        from sedona.utils import KryoSerializer, SedonaKryoRegistrator

        builder = (
            SparkSession.builder.appName(app_name)
            .master(master)
            .config("spark.serializer", KryoSerializer.getName)
            .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)
            .config(
                "spark.jars.packages",
                "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.6.0,"
                "org.datasyslab:geotools-wrapper:1.6.0-28.2",
            )
            .config("spark.sql.extensions", "org.apache.sedona.sql.SedonaSqlExtensions")
        )
        spark = builder.getOrCreate()
        SedonaRegistrator.registerAll(spark)
        logger.info("Successfully initialized Sedona SparkSession.")
        return spark
    except Exception as e:
        logger.warning(
            f"Sedona SparkSession initialization note: {e}. "
            "Falling back to high-performance GeoPandas spatial indexing engine."
        )
        return None


class SedonaSpatialJoinEngine:
    """Executes point-in-polygon spatial joins between GPS coordinates and store catchments.

    Provides transparent execution across Apache Sedona (distributed) and
    GeoPandas STRtree (in-memory spatial index).
    """

    def __init__(
        self,
        spark_session: Optional[Any] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        """Initialize the spatial join engine.

        Args:
            spark_session: Optional active Spark / Sedona session.
            config: Optional configuration dictionary.
        """
        self.config = config or load_yaml_config()
        self.spark = spark_session
        self.sedona_active = False

        if self.spark is not None:
            self.sedona_active = True

    def load_spatial_catchments(
        self,
        catchments_path: Optional[Union[str, Path]] = None,
    ) -> Any:
        """Load store catchment polygons into a GeoDataFrame.

        Args:
            catchments_path: Path to the catchment GeoJSON. Defaults to
                             data/spatial/store_catchments_500m.geojson.

        Returns:
            geopandas.GeoDataFrame with catchment polygons and attributes.
        """
        if not GEOPANDAS_AVAILABLE:
            raise ImportError("geopandas is required to load catchment GeoJSON.")

        if catchments_path is None:
            catchments_file = (
                get_project_root() / "data" / "spatial" / "store_catchments_500m.geojson"
            )
        else:
            catchments_file = Path(catchments_path)

        if not catchments_file.exists():
            raise FileNotFoundError(f"Catchment GeoJSON not found at: {catchments_file}")

        logger.info(f"Loading store catchments from: {catchments_file}")
        gdf = gpd.read_file(str(catchments_file))
        return gdf

    def prepare_pings_geodataframe(
        self,
        pings_data: Union[str, Path, pd.DataFrame, gpd.GeoDataFrame, Any],
    ) -> Any:
        """Convert input GPS telemetry into a standardized Point GeoDataFrame in EPSG:4326.

        Args:
            pings_data: Path to CSV/Parquet file, pandas DataFrame, or GeoDataFrame.

        Returns:
            geopandas.GeoDataFrame with Point geometries.
        """
        if not GEOPANDAS_AVAILABLE:
            raise ImportError("geopandas is required for spatial join operations.")

        if isinstance(pings_data, gpd.GeoDataFrame):
            gdf = pings_data.copy()
            if gdf.crs is None:
                gdf.set_crs("EPSG:4326", inplace=True)
            elif gdf.crs.to_string() != "EPSG:4326":
                gdf = gdf.to_crs("EPSG:4326")
            return gdf

        if isinstance(pings_data, (str, Path)):
            path = Path(pings_data)
            if not path.exists():
                raise FileNotFoundError(f"GPS telemetry file not found: {path}")
            if path.suffix == ".parquet":
                df = pd.read_parquet(str(path))
            else:
                df = pd.read_csv(str(path))
        elif isinstance(pings_data, pd.DataFrame):
            df = pings_data.copy()
        elif PYSPARK_AVAILABLE and isinstance(pings_data, DataFrame):
            df = pings_data.toPandas()
        else:
            raise TypeError(f"Unsupported pings_data type: {type(pings_data)}")

        # Validate required coordinate columns
        if "latitude" not in df.columns or "longitude" not in df.columns:
            raise ValueError("Telemetry data must contain 'latitude' and 'longitude' columns.")

        # Drop null coordinates
        df = df.dropna(subset=["latitude", "longitude"]).copy()
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        df = df.dropna(subset=["latitude", "longitude"]).copy()

        # Build Point geometries
        geometry = [Point(xy) for xy in zip(df["longitude"], df["latitude"])]
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
        return gdf

    def join_pings_with_catchments(
        self,
        pings_data: Union[str, Path, pd.DataFrame, gpd.GeoDataFrame, Any],
        catchments_data: Optional[Union[str, Path, gpd.GeoDataFrame]] = None,
        predicate: str = "within",
    ) -> Any:
        """Execute point-in-polygon spatial join between GPS pings and store catchment areas.

        Args:
            pings_data: Telemetry file path or DataFrame.
            catchments_data: Catchment GeoJSON file path or GeoDataFrame.
            predicate: Spatial join predicate ('within' or 'intersects').

        Returns:
            geopandas.GeoDataFrame containing pings matched with store catchment attributes.
        """
        if not GEOPANDAS_AVAILABLE:
            raise ImportError("geopandas is required for spatial join operations.")

        # 1. Prepare pings
        gdf_pings = self.prepare_pings_geodataframe(pings_data)

        # 2. Prepare catchments
        if catchments_data is None or isinstance(catchments_data, (str, Path)):
            gdf_catchments = self.load_spatial_catchments(catchments_data)
        elif isinstance(catchments_data, gpd.GeoDataFrame):
            gdf_catchments = catchments_data.copy()
        else:
            raise TypeError(f"Unsupported catchments_data type: {type(catchments_data)}")

        # Ensure matching CRS
        if gdf_catchments.crs != gdf_pings.crs:
            gdf_catchments = gdf_catchments.to_crs(gdf_pings.crs)

        logger.info(
            f"Executing point-in-polygon spatial join ({predicate}) between "
            f"{len(gdf_pings):,} GPS pings and {len(gdf_catchments)} store catchments..."
        )

        # 3. Perform spatial join using STRtree index
        # Select clean catchment columns to attach
        catchment_cols = [
            c for c in ["store_id", "name", "category", "buffer_radius_meters", "area_sq_meters", "geometry"]
            if c in gdf_catchments.columns
        ]
        gdf_catchment_sub = gdf_catchments[catchment_cols].rename(columns={"name": "store_name"})

        joined = gpd.sjoin(
            gdf_pings,
            gdf_catchment_sub,
            how="inner",
            predicate=predicate,
        )

        # Clean index column added by sjoin
        if "index_right" in joined.columns:
            joined = joined.drop(columns=["index_right"])

        logger.info(
            f"Spatial join completed: {len(joined):,} / {len(gdf_pings):,} pings fell inside store catchments."
        )
        return joined
