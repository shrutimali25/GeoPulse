"""Store Location Spatial Data & Catchment Area Generation Pipeline for GeoPulse.

Validates retail store coordinates from the POI catalog, constructs Point geometries,
and generates accurate 500-meter metric catchment buffer polygons using GeoPandas,
Shapely, and metric projected coordinate reference systems (EPSG:32643 / UTM Zone 43N).
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from src.common.config_loader import get_project_root, load_yaml_config
from src.common.logger import get_logger
from src.member1_data_eng.generator.poi_catalog import POICatalog, RetailStore

logger = get_logger(__name__)

# Try importing geospatial dependencies
try:
    import geopandas as gpd
    import pyproj
    from shapely.geometry import Point, Polygon, mapping, shape
    from shapely.validation import explain_validity
    GEOPANDAS_AVAILABLE = True
except ImportError:
    GEOPANDAS_AVAILABLE = False
    gpd = Any  # type: ignore

# Default spatial configurations
DEFAULT_BUFFER_RADIUS_METERS = 500.0
DEFAULT_SOURCE_CRS = "EPSG:4326"        # WGS 84 geographic coordinates (degrees)
DEFAULT_PROJECTED_CRS = "EPSG:32643"    # WGS 84 / UTM Zone 43N (Bengaluru metric projection, meters)
DEFAULT_OUTPUT_CRS = "EPSG:4326"       # WGS 84 for GeoJSON web standard


@dataclass
class StoreValidationResult:
    """Result of validating retail store records."""

    is_valid: bool
    store_count: int
    unique_store_ids: List[str]
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return asdict(self)


@dataclass
class CatchmentSummary:
    """Summary of 500m catchment area generation."""

    total_stores: int
    processed_stores: int
    buffer_radius_meters: float
    source_crs: str
    projected_crs: str
    output_crs: str
    all_geometries_valid: bool
    store_ids: List[str]
    stores: List[Dict[str, Any]] = field(default_factory=list)
    output_files: Dict[str, str] = field(default_factory=dict)
    validation: Dict[str, Any] = field(default_factory=dict)
    generated_at_utc: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> Dict[str, Any]:
        """Convert summary to dictionary."""
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        """Serialize summary to formatted JSON string."""
        return json.dumps(self.to_dict(), indent=indent)


def validate_store_records(
    stores: Union[List[RetailStore], List[Dict[str, Any]]],
    strict: bool = True,
    study_zone_bbox: Optional[Dict[str, float]] = None,
) -> StoreValidationResult:
    """Validate retail store records for uniqueness, required fields, and coordinate ranges.

    Args:
        stores: List of RetailStore objects or dictionaries.
        strict: If True, raises ValueError upon any validation failure.
        study_zone_bbox: Optional bounding box dict (min_lat, max_lat, min_lon, max_lon)
                         to check that stores fall within the study zone.

    Returns:
        StoreValidationResult dataclass with validation status and error details.

    Raises:
        ValueError: If strict=True and any validation error is detected.
    """
    errors: List[str] = []
    seen_ids: set = set()
    unique_ids: List[str] = []

    if not stores:
        err = "No store records provided for validation (empty store list)."
        errors.append(err)
        if strict:
            raise ValueError(err)
        return StoreValidationResult(is_valid=False, store_count=0, unique_store_ids=[], errors=errors)

    for idx, s in enumerate(stores):
        # Extract attributes whether dataclass or dictionary
        if isinstance(s, RetailStore):
            store_id = s.store_id
            name = s.name
            category = s.category
            lat = s.latitude
            lon = s.longitude
        elif isinstance(s, dict):
            store_id = s.get("store_id")
            name = s.get("name")
            category = s.get("category")
            lat = s.get("latitude")
            lon = s.get("longitude")
        else:
            errors.append(f"Store at index {idx} has invalid type: {type(s)}")
            continue

        # Check required fields
        if not store_id or not isinstance(store_id, str) or not store_id.strip():
            errors.append(f"Store at index {idx} has missing or empty 'store_id'.")
            continue

        store_id = store_id.strip()

        # Check uniqueness
        if store_id in seen_ids:
            errors.append(f"Duplicate store_id detected: '{store_id}' at index {idx}.")
        else:
            seen_ids.add(store_id)
            unique_ids.append(store_id)

        # Check name & category
        if not name or not isinstance(name, str) or not name.strip():
            errors.append(f"Store '{store_id}' has missing or empty 'name'.")
        if not category or not isinstance(category, str) or not category.strip():
            errors.append(f"Store '{store_id}' has missing or empty 'category'.")

        # Check latitude and longitude
        try:
            lat_f = float(lat)
            lon_f = float(lon)
        except (TypeError, ValueError):
            errors.append(
                f"Store '{store_id}' has non-numeric coordinates: lat={lat}, lon={lon}."
            )
            continue

        if not (-90.0 <= lat_f <= 90.0):
            errors.append(
                f"Store '{store_id}' latitude out of bounds [-90.0, 90.0]: {lat_f}."
            )
        if not (-180.0 <= lon_f <= 180.0):
            errors.append(
                f"Store '{store_id}' longitude out of bounds [-180.0, 180.0]: {lon_f}."
            )

        # Optional study zone bounding box check
        if study_zone_bbox:
            min_lat = study_zone_bbox.get("min_lat", -90.0)
            max_lat = study_zone_bbox.get("max_lat", 90.0)
            min_lon = study_zone_bbox.get("min_lon", -180.0)
            max_lon = study_zone_bbox.get("max_lon", 180.0)
            if not (min_lat <= lat_f <= max_lat and min_lon <= lon_f <= max_lon):
                errors.append(
                    f"Store '{store_id}' at ({lat_f}, {lon_f}) lies outside the configured "
                    f"study zone bbox [{min_lat}, {max_lat}, {min_lon}, {max_lon}]."
                )

    is_valid = len(errors) == 0
    if not is_valid and strict:
        joined_errors = "; ".join(errors)
        raise ValueError(f"Store validation failed with {len(errors)} error(s): {joined_errors}")

    return StoreValidationResult(
        is_valid=is_valid,
        store_count=len(stores),
        unique_store_ids=unique_ids,
        errors=errors,
    )


class StoreCatchmentGenerator:
    """Generates accurate metric spatial catchment buffer polygons around retail store locations.

    Uses GeoPandas and Shapely with metric projection (EPSG:32643 / UTM 43N) to ensure
    true circular geodesic buffers (e.g. 500m) rather than distorted degree buffers.
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        poi_catalog: Optional[POICatalog] = None,
        projected_crs: str = DEFAULT_PROJECTED_CRS,
        source_crs: str = DEFAULT_SOURCE_CRS,
        output_crs: str = DEFAULT_OUTPUT_CRS,
    ):
        """Initialize StoreCatchmentGenerator with configuration and POI catalog.

        Args:
            config: Optional configuration dictionary. Defaults to base.yaml.
            poi_catalog: Optional POICatalog instance.
            projected_crs: Projected coordinate system with meter units (default EPSG:32643).
            source_crs: Source CRS of the POI catalog coordinates (default EPSG:4326).
            output_crs: Desired output CRS for GeoJSON serialization (default EPSG:4326).
        """
        self.config = config or load_yaml_config()
        self.poi_catalog = poi_catalog or POICatalog(self.config)
        self.projected_crs = projected_crs
        self.source_crs = source_crs
        self.output_crs = output_crs

        # Extract study zone bounding box if available
        self.study_zone_bbox = self.config.get("study_zone", {}).get("bounding_box")

    def get_validated_stores(self, strict: bool = True) -> List[RetailStore]:
        """Retrieve and validate all stores from the POI catalog.

        Args:
            strict: If True, raises ValueError if validation fails.

        Returns:
            List of validated RetailStore objects.
        """
        stores = self.poi_catalog.get_all_stores()
        validate_store_records(stores, strict=strict, study_zone_bbox=self.study_zone_bbox)
        return stores

    def create_stores_geodataframe(
        self,
        stores: Optional[List[RetailStore]] = None,
    ) -> Any:
        """Convert retail store locations into a GeoPandas GeoDataFrame with Point geometries.

        Args:
            stores: Optional list of RetailStore objects. If omitted, uses catalog stores.

        Returns:
            geopandas.GeoDataFrame with Point geometries in source CRS (EPSG:4326).
        """
        if not GEOPANDAS_AVAILABLE:
            raise ImportError(
                "geopandas and shapely are required for spatial catchment processing. "
                "Ensure you run inside the WSL environment (.venv-wsl312) where they are installed."
            )

        store_list = stores if stores is not None else self.get_validated_stores()

        rows = []
        for s in store_list:
            pt = Point(float(s.longitude), float(s.latitude))
            rows.append({
                "store_id": s.store_id,
                "name": s.name,
                "category": s.category,
                "latitude": float(s.latitude),
                "longitude": float(s.longitude),
                "polygon_buffer_meters": float(s.polygon_buffer_meters),
                "geometry": pt,
            })

        gdf = gpd.GeoDataFrame(rows, crs=self.source_crs)
        logger.info(
            f"Created Store GeoDataFrame with {len(gdf)} stores in CRS {self.source_crs}."
        )
        return gdf

    def generate_catchment_areas(
        self,
        buffer_radius_meters: float = DEFAULT_BUFFER_RADIUS_METERS,
        stores: Optional[List[RetailStore]] = None,
        projected_crs: Optional[str] = None,
    ) -> Any:
        """Generate accurate metric buffer catchment polygons around store locations.

        Projected CRS with meter units is strictly used to ensure accurate metric buffers
        on the ground, avoiding degree-based geometric distortion.

        Args:
            buffer_radius_meters: Radius in meters (default 500.0).
            stores: Optional list of RetailStore objects.
            projected_crs: Metric CRS for buffering (default EPSG:32643 / UTM Zone 43N).

        Returns:
            geopandas.GeoDataFrame with Polygon catchment geometries in output CRS (EPSG:4326).
        """
        if not GEOPANDAS_AVAILABLE:
            raise ImportError(
                "geopandas and shapely are required for spatial catchment processing."
            )

        if buffer_radius_meters <= 0:
            raise ValueError(
                f"Buffer radius must be strictly positive, got: {buffer_radius_meters}m"
            )

        proj_crs = projected_crs or self.projected_crs

        # 1. Create Point GeoDataFrame in WGS84
        gdf_points = self.create_stores_geodataframe(stores=stores)

        # 2. Guard against buffering directly in angular degrees
        if gdf_points.crs.is_geographic and proj_crs == self.source_crs:
            raise ValueError(
                "Cannot generate metric buffer directly in angular degrees (EPSG:4326). "
                f"An appropriate projected CRS with meter units (e.g., {DEFAULT_PROJECTED_CRS}) must be used."
            )

        # 3. Reproject to metric projected CRS (e.g. UTM Zone 43N for Bengaluru)
        logger.info(
            f"Reprojecting {len(gdf_points)} store points from {self.source_crs} to {proj_crs}..."
        )
        gdf_projected = gdf_points.to_crs(proj_crs)

        # 4. Generate metric circular buffers (quad_segs=16 yields 64 segments per full circle)
        logger.info(
            f"Generating {buffer_radius_meters}m catchment buffers in projected CRS {proj_crs}..."
        )
        try:
            buffered_geoms = gdf_projected.geometry.buffer(buffer_radius_meters, quad_segs=16)
        except TypeError:
            buffered_geoms = gdf_projected.geometry.buffer(buffer_radius_meters, resolution=16)

        # 5. Validate projected polygon geometries and compute metric area
        metric_areas = []
        for idx, poly in enumerate(buffered_geoms):
            if not poly.is_valid or poly.is_empty:
                reason = explain_validity(poly) if hasattr(poly, "is_valid") else "empty/invalid"
                raise ValueError(
                    f"Generated buffer polygon for store at index {idx} is invalid: {reason}"
                )
            metric_areas.append(round(float(poly.area), 2))

        # 6. Construct catchment GeoDataFrame in projected CRS
        gdf_catchment_proj = gpd.GeoDataFrame(
            {
                "store_id": gdf_projected["store_id"],
                "name": gdf_projected["name"],
                "category": gdf_projected["category"],
                "latitude": gdf_projected["latitude"],
                "longitude": gdf_projected["longitude"],
                "buffer_radius_meters": float(buffer_radius_meters),
                "buffer_crs": str(proj_crs),
                "area_sq_meters": metric_areas,
            },
            geometry=buffered_geoms,
            crs=proj_crs,
        )

        # 7. Reproject catchment polygons back to output CRS (EPSG:4326) for GeoJSON standard
        logger.info(
            f"Reprojecting catchment polygons from {proj_crs} to output CRS {self.output_crs}..."
        )
        gdf_catchment_wgs84 = gdf_catchment_proj.to_crs(self.output_crs)

        # 8. Verify all reprojected geometries remain valid
        for idx, row in gdf_catchment_wgs84.iterrows():
            geom = row.geometry
            if not geom.is_valid:
                logger.warning(
                    f"Catchment geometry for store '{row['store_id']}' required validity repair."
                )
                from shapely import make_valid
                gdf_catchment_wgs84.at[idx, "geometry"] = make_valid(geom)

        logger.info(
            f"Successfully generated {len(gdf_catchment_wgs84)} valid catchment polygons."
        )
        return gdf_catchment_wgs84

    def save_outputs(
        self,
        gdf_stores: Any,
        gdf_catchments: Any,
        summary: CatchmentSummary,
        spatial_dir: Optional[Union[str, Path]] = None,
    ) -> Dict[str, str]:
        """Save store locations, catchment polygons, and summary report to disk.

        Args:
            gdf_stores: Store locations GeoDataFrame.
            gdf_catchments: Catchment polygons GeoDataFrame.
            summary: CatchmentSummary dataclass instance.
            spatial_dir: Target spatial directory. Defaults to data/spatial/.

        Returns:
            Dictionary of saved file paths.
        """
        if spatial_dir is None:
            target_dir = get_project_root() / "data" / "spatial"
        else:
            target_dir = Path(spatial_dir)

        target_dir.mkdir(parents=True, exist_ok=True)

        store_geojson_path = target_dir / "store_locations.geojson"
        catchment_geojson_path = target_dir / "store_catchments_500m.geojson"
        summary_path = target_dir / "catchment_generation_summary.json"

        # 1. Save store locations GeoJSON
        logger.info(f"Saving store locations GeoJSON to: {store_geojson_path}")
        gdf_stores.to_file(str(store_geojson_path), driver="GeoJSON")

        # 2. Save 500m catchments GeoJSON
        logger.info(f"Saving 500m catchments GeoJSON to: {catchment_geojson_path}")
        gdf_catchments.to_file(str(catchment_geojson_path), driver="GeoJSON")

        # 3. Update summary with output paths
        written_files = {
            "store_locations_geojson": str(store_geojson_path),
            "store_catchments_geojson": str(catchment_geojson_path),
            "summary_json": str(summary_path),
        }
        summary.output_files = written_files

        # 4. Save summary JSON
        with open(summary_path, "w", encoding="utf-8") as f:
            f.write(summary.to_json(indent=2))
        logger.info(f"Saved catchment summary to: {summary_path}")

        return written_files

    def run_pipeline(
        self,
        buffer_radius_meters: float = DEFAULT_BUFFER_RADIUS_METERS,
        spatial_dir: Optional[Union[str, Path]] = None,
        projected_crs: Optional[str] = None,
    ) -> CatchmentSummary:
        """Execute end-to-end Day 5 catchment generation pipeline.

        1. Validates 5 retail store POIs.
        2. Constructs Point spatial GeoDataFrame.
        3. Projects to UTM Zone 43N and computes 500m circular catchment buffers.
        4. Re-projects to WGS84 (EPSG:4326) and verifies geometric validity.
        5. Persists GeoJSON datasets and summary metrics.

        Args:
            buffer_radius_meters: Buffer radius in meters (default 500.0).
            spatial_dir: Destination folder (default data/spatial/).
            projected_crs: Projected metric CRS (default EPSG:32643).

        Returns:
            CatchmentSummary detailing execution metrics.
        """
        proj_crs = projected_crs or self.projected_crs

        # 1. Validate stores
        stores = self.get_validated_stores(strict=True)
        store_validation = validate_store_records(stores, strict=True, study_zone_bbox=self.study_zone_bbox)

        # 2. Build Point GeoDataFrame
        gdf_stores = self.create_stores_geodataframe(stores=stores)

        # 3. Generate catchment polygons
        gdf_catchments = self.generate_catchment_areas(
            buffer_radius_meters=buffer_radius_meters,
            stores=stores,
            projected_crs=proj_crs,
        )

        # 4. Extract per-store summary info
        store_summaries: List[Dict[str, Any]] = []
        for _, row in gdf_catchments.iterrows():
            bounds = row.geometry.bounds  # (minx, miny, maxx, maxy) in WGS84
            store_summaries.append({
                "store_id": row["store_id"],
                "name": row["name"],
                "category": row["category"],
                "center_latitude": row["latitude"],
                "center_longitude": row["longitude"],
                "buffer_radius_meters": row["buffer_radius_meters"],
                "area_sq_meters": row["area_sq_meters"],
                "geometry_type": row.geometry.geom_type,
                "is_valid": row.geometry.is_valid,
                "bounds_wgs84": {
                    "min_lon": round(bounds[0], 6),
                    "min_lat": round(bounds[1], 6),
                    "max_lon": round(bounds[2], 6),
                    "max_lat": round(bounds[3], 6),
                },
            })

        all_valid = all(s["is_valid"] for s in store_summaries)

        summary = CatchmentSummary(
            total_stores=len(stores),
            processed_stores=len(gdf_catchments),
            buffer_radius_meters=float(buffer_radius_meters),
            source_crs=self.source_crs,
            projected_crs=proj_crs,
            output_crs=self.output_crs,
            all_geometries_valid=all_valid,
            store_ids=[s.store_id for s in stores],
            stores=store_summaries,
            validation=store_validation.to_dict(),
        )

        # 5. Persist output files
        self.save_outputs(
            gdf_stores=gdf_stores,
            gdf_catchments=gdf_catchments,
            summary=summary,
            spatial_dir=spatial_dir,
        )

        logger.info(
            f"Day 5 pipeline complete: {summary.processed_stores} stores processed, "
            f"{buffer_radius_meters}m catchment buffers generated, CRS: {proj_crs} -> {self.output_crs}."
        )
        return summary


if __name__ == "__main__":
    generator = StoreCatchmentGenerator()
    result = generator.run_pipeline()
    print("\n" + "=" * 70)
    print(" GeoPulse Day 5: Store Locations & 500m Catchment Summary")
    print("=" * 70)
    print(result.to_json(indent=2))
