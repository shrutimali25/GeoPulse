"""Tests for configuration loading and POI catalog."""

import pytest
from src.common.config_loader import load_yaml_config
from src.member1_data_eng.generator.poi_catalog import POICatalog


def test_load_yaml_config():
    """Verify base YAML configuration loads with expected keys."""
    config = load_yaml_config()
    assert "project" in config
    assert config["project"]["name"] == "GeoPulse"
    assert "study_zone" in config
    assert "bounding_box" in config["study_zone"]
    assert "retail_stores" in config


def test_poi_catalog_stores():
    """Verify POI catalog parses retail stores correctly."""
    catalog = POICatalog()
    stores = catalog.get_all_stores()
    assert len(stores) >= 3

    forum_mall = catalog.get_store_by_id("RET_MALL_FORUM_001")
    assert forum_mall is not None
    assert forum_mall.name == "Forum Mall Koramangala"
    assert forum_mall.category == "Shopping Mall"
    assert forum_mall.latitude == pytest.approx(12.9345, rel=1e-3)
    assert forum_mall.longitude == pytest.approx(77.6110, rel=1e-3)


def test_geojson_boundary_loading():
    """Verify sample GeoJSON file is valid and readable."""
    catalog = POICatalog()
    geojson = catalog.load_geojson_features()
    assert geojson.get("type") == "FeatureCollection"
    assert "features" in geojson
    assert len(geojson["features"]) >= 1
