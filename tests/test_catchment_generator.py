"""Unit tests for retail store locations and 500m catchment area generation.

Verifies:
- Identification and validation of all 5 retail store POIs
- Unique store IDs and coordinate boundary enforcement
- Accurate 500m metric buffering via projected CRS (EPSG:32643 / UTM 43N)
- Rejection of naive degree-based buffering
- GeoJSON output serialization, loadability, and geometric validity
- Diagnostic reporting when dependencies are unavailable
"""

import json
import math
from pathlib import Path
import pytest

from src.common.config_loader import get_project_root, load_yaml_config
from src.member1_data_eng.generator.poi_catalog import POICatalog, RetailStore
from src.member1_data_eng.spatial_processing.catchment_generator import (
    DEFAULT_BUFFER_RADIUS_METERS,
    DEFAULT_PROJECTED_CRS,
    GEOPANDAS_AVAILABLE,
    CatchmentSummary,
    StoreCatchmentGenerator,
    StoreValidationResult,
    validate_store_records,
)

if GEOPANDAS_AVAILABLE:
    import geopandas as gpd
    from shapely.geometry import Point, Polygon


def test_poi_catalog_contains_5_stores():
    """Verify that POI catalog contains all 5 designated retail stores."""
    catalog = POICatalog()
    stores = catalog.get_all_stores()
    assert len(stores) == 5

    expected_store_ids = {
        "RET_MALL_FORUM_001",
        "RET_CAFE_STARBUCKS_002",
        "RET_ELEC_CROMA_003",
        "RET_GROC_NATURES_BASKET_004",
        "RET_FASH_ZARA_005",
    }
    actual_ids = {s.store_id for s in stores}
    assert actual_ids == expected_store_ids


def test_validate_store_records_success():
    """Verify that the 5 catalog stores pass all validation checks."""
    catalog = POICatalog()
    stores = catalog.get_all_stores()
    result = validate_store_records(stores, strict=True)

    assert result.is_valid is True
    assert result.store_count == 5
    assert len(result.unique_store_ids) == 5
    assert len(result.errors) == 0


def test_validate_store_records_detects_duplicates():
    """Verify duplicate store IDs are rejected."""
    duplicate_stores = [
        RetailStore(
            store_id="RET_MALL_FORUM_001",
            name="Forum Mall A",
            category="Shopping Mall",
            latitude=12.9345,
            longitude=77.6110,
            polygon_buffer_meters=65.0,
        ),
        RetailStore(
            store_id="RET_MALL_FORUM_001",
            name="Forum Mall B",
            category="Shopping Mall",
            latitude=12.9346,
            longitude=77.6111,
            polygon_buffer_meters=65.0,
        ),
    ]

    # Strict mode should raise ValueError
    with pytest.raises(ValueError, match="Duplicate store_id detected"):
        validate_store_records(duplicate_stores, strict=True)

    # Non-strict mode should return is_valid=False with diagnostic error
    result = validate_store_records(duplicate_stores, strict=False)
    assert result.is_valid is False
    assert any("Duplicate store_id detected" in err for err in result.errors)


@pytest.mark.parametrize(
    "invalid_lat,invalid_lon,expected_msg",
    [
        (95.0, 77.62, "latitude out of bounds"),
        (-95.0, 77.62, "latitude out of bounds"),
        (12.93, 190.0, "longitude out of bounds"),
        (12.93, -190.0, "longitude out of bounds"),
    ],
)
def test_validate_store_records_invalid_coordinates(invalid_lat, invalid_lon, expected_msg):
    """Verify out-of-bounds geographic coordinates are rejected."""
    bad_store = [
        {
            "store_id": "RET_TEST_001",
            "name": "Invalid Coord Store",
            "category": "Test",
            "latitude": invalid_lat,
            "longitude": invalid_lon,
        }
    ]

    with pytest.raises(ValueError, match=expected_msg):
        validate_store_records(bad_store, strict=True)


def test_validate_store_records_missing_required_fields():
    """Verify missing store_id, name, or category raises descriptive error."""
    bad_stores = [
        {"store_id": "", "name": "Store", "category": "Cat", "latitude": 12.9, "longitude": 77.6},
        {"store_id": "ST_01", "name": "", "category": "Cat", "latitude": 12.9, "longitude": 77.6},
        {"store_id": "ST_02", "name": "Store", "category": "", "latitude": 12.9, "longitude": 77.6},
    ]

    result = validate_store_records(bad_stores, strict=False)
    assert result.is_valid is False
    assert len(result.errors) >= 3


def test_validate_empty_store_list():
    """Verify empty store list is rejected."""
    with pytest.raises(ValueError, match="No store records provided"):
        validate_store_records([], strict=True)


@pytest.mark.skipif(not GEOPANDAS_AVAILABLE, reason="geopandas is not installed in the active environment")
def test_create_stores_geodataframe():
    """Verify Point GeoDataFrame construction with correct CRS and coordinates."""
    generator = StoreCatchmentGenerator()
    gdf_stores = generator.create_stores_geodataframe()

    assert len(gdf_stores) == 5
    assert gdf_stores.crs.to_string() == "EPSG:4326"
    assert "geometry" in gdf_stores.columns
    assert "store_id" in gdf_stores.columns

    for _, row in gdf_stores.iterrows():
        geom = row.geometry
        assert isinstance(geom, Point)
        assert geom.is_valid
        # Point(x=lon, y=lat)
        assert geom.x == pytest.approx(row["longitude"], abs=1e-5)
        assert geom.y == pytest.approx(row["latitude"], abs=1e-5)


@pytest.mark.skipif(not GEOPANDAS_AVAILABLE, reason="geopandas is not installed in the active environment")
def test_generate_500m_catchment_polygons():
    """Verify 500m catchment area generation around each retail store."""
    generator = StoreCatchmentGenerator()
    gdf_catchments = generator.generate_catchment_areas(buffer_radius_meters=500.0)

    # 1. Store and polygon count
    assert len(gdf_catchments) == 5
    assert len(gdf_catchments["store_id"].unique()) == 5

    # 2. CRS check
    assert gdf_catchments.crs.to_string() == "EPSG:4326"

    # 3. Geometry validity
    for _, row in gdf_catchments.iterrows():
        poly = row.geometry
        assert isinstance(poly, Polygon)
        assert poly.is_valid
        assert not poly.is_empty
        assert row["buffer_radius_meters"] == 500.0
        assert row["buffer_crs"] == DEFAULT_PROJECTED_CRS

        # Area in square meters: circle area = pi * r^2 = pi * 500^2 ~ 785,398 m2
        expected_area = math.pi * (500.0**2)
        assert row["area_sq_meters"] == pytest.approx(expected_area, rel=0.01)


@pytest.mark.skipif(not GEOPANDAS_AVAILABLE, reason="geopandas is not installed in the active environment")
def test_projected_crs_buffering_accuracy():
    """Verify buffering happens in metric units and gives exact 500m radius in projected space."""
    generator = StoreCatchmentGenerator()
    gdf_stores = generator.create_stores_geodataframe()

    # Reproject to UTM Zone 43N
    gdf_proj = gdf_stores.to_crs(DEFAULT_PROJECTED_CRS)
    buf_proj = gdf_proj.geometry.buffer(500.0, quad_segs=16)

    for idx, geom in enumerate(buf_proj):
        pt = gdf_proj.geometry.iloc[idx]
        # Distance from center point to any boundary point should be exactly 500m (+/- 1m approximation)
        exterior_coords = list(geom.exterior.coords)
        for coord in exterior_coords[:10]:
            dist = pt.distance(Point(coord))
            assert dist == pytest.approx(500.0, rel=0.005)


@pytest.mark.skipif(not GEOPANDAS_AVAILABLE, reason="geopandas is not installed in the active environment")
def test_rejection_of_angular_degree_buffering():
    """Verify generator rejects buffering directly on geographic degrees (EPSG:4326)."""
    generator = StoreCatchmentGenerator(
        projected_crs="EPSG:4326",
        source_crs="EPSG:4326",
    )
    with pytest.raises(ValueError, match="Cannot generate metric buffer directly in angular degrees"):
        generator.generate_catchment_areas(buffer_radius_meters=500.0)


@pytest.mark.skipif(not GEOPANDAS_AVAILABLE, reason="geopandas is not installed in the active environment")
def test_rejection_of_non_positive_buffer_radius():
    """Verify zero or negative buffer radius is rejected."""
    generator = StoreCatchmentGenerator()
    with pytest.raises(ValueError, match="Buffer radius must be strictly positive"):
        generator.generate_catchment_areas(buffer_radius_meters=0.0)

    with pytest.raises(ValueError, match="Buffer radius must be strictly positive"):
        generator.generate_catchment_areas(buffer_radius_meters=-50.0)


@pytest.mark.skipif(not GEOPANDAS_AVAILABLE, reason="geopandas is not installed in the active environment")
def test_end_to_end_pipeline_and_file_outputs(tmp_path):
    """Verify full Day 5 pipeline execution, file persistence, and GeoJSON loadability."""
    generator = StoreCatchmentGenerator()
    summary = generator.run_pipeline(
        buffer_radius_meters=500.0,
        spatial_dir=tmp_path,
    )

    # 1. Summary validation
    assert summary.total_stores == 5
    assert summary.processed_stores == 5
    assert summary.buffer_radius_meters == 500.0
    assert summary.all_geometries_valid is True
    assert summary.projected_crs == DEFAULT_PROJECTED_CRS
    assert len(summary.store_ids) == 5

    # 2. Output file existence
    store_file = Path(summary.output_files["store_locations_geojson"])
    catchment_file = Path(summary.output_files["store_catchments_geojson"])
    summary_file = Path(summary.output_files["summary_json"])

    assert store_file.exists()
    assert catchment_file.exists()
    assert summary_file.exists()

    # 3. Load and verify store_locations.geojson with GeoPandas
    loaded_stores = gpd.read_file(str(store_file))
    assert len(loaded_stores) == 5
    assert loaded_stores.crs.to_string() == "EPSG:4326"
    assert set(loaded_stores["store_id"]) == set(summary.store_ids)
    assert all(geom.geom_type == "Point" for geom in loaded_stores.geometry)

    # 4. Load and verify store_catchments_500m.geojson with GeoPandas
    loaded_catchments = gpd.read_file(str(catchment_file))
    assert len(loaded_catchments) == 5
    assert loaded_catchments.crs.to_string() == "EPSG:4326"
    assert set(loaded_catchments["store_id"]) == set(summary.store_ids)
    assert all(geom.geom_type == "Polygon" for geom in loaded_catchments.geometry)
    assert all(geom.is_valid for geom in loaded_catchments.geometry)

    # 5. Load and verify summary JSON
    with open(summary_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["total_stores"] == 5
    assert data["processed_stores"] == 5
    assert data["validation"]["is_valid"] is True


@pytest.mark.skipif(not GEOPANDAS_AVAILABLE, reason="geopandas is not installed in the active environment")
def test_custom_catchment_buffer_radius(tmp_path):
    """Verify pipeline functions with custom buffer radius (e.g. 250m) and scales area appropriately."""
    generator = StoreCatchmentGenerator()
    summary_250 = generator.run_pipeline(
        buffer_radius_meters=250.0,
        spatial_dir=tmp_path / "radius_250",
    )
    assert summary_250.buffer_radius_meters == 250.0

    catchment_file = Path(summary_250.output_files["store_catchments_geojson"])
    loaded_250 = gpd.read_file(str(catchment_file))

    # Area of 250m circle should be 1/4 of 500m circle (r^2 ratio)
    expected_250_area = math.pi * (250.0**2)
    for area in loaded_250["area_sq_meters"]:
        assert area == pytest.approx(expected_250_area, rel=0.01)
