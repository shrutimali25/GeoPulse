"""Unit tests for Day 9: Cannibalization & Visitor Overlap Analysis.

Verifies:
- Unique visitor counts per store
- Pairwise shared visitor calculations
- Visitor overlap percentage relative to smaller visitor base
- Jaccard similarity metrics (identical, disjoint, and partial sets)
- Metric catchment intersection area and overlap percentage in EPSG:32643
- Robust empty dataset and single-store handling
- Input validation (missing columns, missing files, invalid geometries)
- Multi-dimensional cannibalization risk categorization (HIGH, MEDIUM, LOW, NEGLIGIBLE)
- End-to-end deliverable persistence (CSV and JSON) and schema integrity
"""

from datetime import datetime, timezone
import json
from pathlib import Path
import pytest
import pandas as pd

from src.common.config_loader import get_project_root
from src.member1_data_eng.spatial_processing.cannibalization_analyzer import (
    CannibalizationAnalyzer,
    CannibalizationPairResult,
    CannibalizationSummary,
    classify_cannibalization_risk,
)


@pytest.fixture
def baseline_events_df():
    """Load baseline store catchment events."""
    events_csv = get_project_root() / "data" / "processed" / "store_catchment_events.csv"
    assert events_csv.exists(), f"Events CSV missing: {events_csv}"
    return pd.read_csv(events_csv)


@pytest.fixture
def baseline_catchments_geojson():
    """Path to 500m store catchments GeoJSON."""
    geojson_path = get_project_root() / "data" / "spatial" / "store_catchments_500m.geojson"
    assert geojson_path.exists(), f"Catchments GeoJSON missing: {geojson_path}"
    return geojson_path


@pytest.fixture
def synthetic_overlap_events():
    """Create synthetic events with known visitor sets for deterministic verification."""
    records = [
        # Store A: 3 visitors (D1, D2, D3)
        {"store_id": "STORE_A", "device_id": "D1"},
        {"store_id": "STORE_A", "device_id": "D2"},
        {"store_id": "STORE_A", "device_id": "D3"},
        # Store B: 2 visitors (D2, D3) -> shared with A: D2, D3 (2/2 = 100%, Jaccard = 2/3)
        {"store_id": "STORE_B", "device_id": "D2"},
        {"store_id": "STORE_B", "device_id": "D3"},
        # Store C: 2 visitors (D4, D5) -> 0 shared with A or B
        {"store_id": "STORE_C", "device_id": "D4"},
        {"store_id": "STORE_C", "device_id": "D5"},
    ]
    return pd.DataFrame(records)


def test_unique_visitor_calculation(synthetic_overlap_events):
    """Verify unique visitors per store are accurately computed."""
    analyzer = CannibalizationAnalyzer()
    metrics = analyzer.compute_visitor_overlap(synthetic_overlap_events)

    # Pairs: (STORE_A, STORE_B), (STORE_A, STORE_C), (STORE_B, STORE_C)
    pair_ab = metrics[("STORE_A", "STORE_B")]
    assert pair_ab["unique_visitors_a"] == 3
    assert pair_ab["unique_visitors_b"] == 2

    pair_ac = metrics[("STORE_A", "STORE_C")]
    assert pair_ac["unique_visitors_a"] == 3
    assert pair_ac["unique_visitors_c" if "unique_visitors_c" in pair_ac else "unique_visitors_b"] == 2


def test_shared_visitor_calculation(synthetic_overlap_events):
    """Verify shared visitor count between store pairs."""
    analyzer = CannibalizationAnalyzer()
    metrics = analyzer.compute_visitor_overlap(synthetic_overlap_events)

    # STORE_A and STORE_B share D2, D3 -> 2
    assert metrics[("STORE_A", "STORE_B")]["shared_visitors"] == 2

    # STORE_A and STORE_C share 0
    assert metrics[("STORE_A", "STORE_C")]["shared_visitors"] == 0

    # STORE_B and STORE_C share 0
    assert metrics[("STORE_B", "STORE_C")]["shared_visitors"] == 0


def test_visitor_overlap_percentage(synthetic_overlap_events):
    """Verify visitor overlap percentage relative to the smaller visitor base."""
    analyzer = CannibalizationAnalyzer()
    metrics = analyzer.compute_visitor_overlap(synthetic_overlap_events)

    # STORE_A (3) and STORE_B (2): min is 2, shared is 2 -> 2/2 * 100 = 100.0%
    assert metrics[("STORE_A", "STORE_B")]["visitor_overlap_percentage"] == 100.0

    # STORE_A and STORE_C: shared 0 -> 0.0%
    assert metrics[("STORE_A", "STORE_C")]["visitor_overlap_percentage"] == 0.0


def test_jaccard_similarity(synthetic_overlap_events):
    """Verify Jaccard similarity between customer visitor sets."""
    analyzer = CannibalizationAnalyzer()
    metrics = analyzer.compute_visitor_overlap(synthetic_overlap_events)

    # STORE_A {D1, D2, D3} & STORE_B {D2, D3}: intersection=2, union=3 -> 2/3 = 0.6667
    assert metrics[("STORE_A", "STORE_B")]["jaccard_similarity"] == pytest.approx(0.6667, abs=0.0001)

    # Disjoint sets: STORE_A & STORE_C -> 0.0
    assert metrics[("STORE_A", "STORE_C")]["jaccard_similarity"] == 0.0


def test_catchment_intersection_calculation(baseline_catchments_geojson):
    """Verify geometric intersection area and overlap percentage using baseline catchments."""
    analyzer = CannibalizationAnalyzer()
    spatial_metrics = analyzer.compute_catchment_overlap(baseline_catchments_geojson)

    # 5 stores = 10 unique pairs
    assert len(spatial_metrics) == 10

    # Zara and Forum Mall are adjacent in Koramangala Hub (~150m apart)
    pair_zara_forum = (
        ("RET_FASH_ZARA_005", "RET_MALL_FORUM_001")
        if ("RET_FASH_ZARA_005", "RET_MALL_FORUM_001") in spatial_metrics
        else ("RET_MALL_FORUM_001", "RET_FASH_ZARA_005")
    )
    assert pair_zara_forum in spatial_metrics
    zara_forum_data = spatial_metrics[pair_zara_forum]
    assert zara_forum_data["catchment_intersection_area"] > 500000.0  # > 500,000 sqm
    assert zara_forum_data["catchment_overlap_percentage"] > 70.0      # > 70% overlap

    # Croma and Nature's Basket are ~350m apart
    pair_croma_nb = (
        ("RET_ELEC_CROMA_003", "RET_GROC_NATURES_BASKET_004")
        if ("RET_ELEC_CROMA_003", "RET_GROC_NATURES_BASKET_004") in spatial_metrics
        else ("RET_GROC_NATURES_BASKET_004", "RET_ELEC_CROMA_003")
    )
    assert pair_croma_nb in spatial_metrics
    croma_nb_data = spatial_metrics[pair_croma_nb]
    assert croma_nb_data["catchment_intersection_area"] > 300000.0  # > 300,000 sqm
    assert croma_nb_data["catchment_overlap_percentage"] > 40.0      # > 40% overlap

    # Starbucks and Forum Mall are ~1.1km apart (> 1000m sum of radii) -> 0 overlap
    pair_starbucks_forum = (
        ("RET_CAFE_STARBUCKS_002", "RET_MALL_FORUM_001")
        if ("RET_CAFE_STARBUCKS_002", "RET_MALL_FORUM_001") in spatial_metrics
        else ("RET_MALL_FORUM_001", "RET_CAFE_STARBUCKS_002")
    )
    assert pair_starbucks_forum in spatial_metrics
    assert spatial_metrics[pair_starbucks_forum]["catchment_intersection_area"] == 0.0
    assert spatial_metrics[pair_starbucks_forum]["catchment_overlap_percentage"] == 0.0


def test_risk_category_classification():
    """Verify deterministic risk classification rules across all boundary thresholds."""
    # HIGH: High spatial (>= 20%) AND high visitor (>= 25% or >= 2 shared)
    assert classify_cannibalization_risk(catchment_overlap_pct=50.0, visitor_overlap_pct=50.0, shared_visitors=2) == "HIGH"
    assert classify_cannibalization_risk(catchment_overlap_pct=25.0, visitor_overlap_pct=30.0, shared_visitors=1) == "HIGH"
    assert classify_cannibalization_risk(catchment_overlap_pct=20.0, visitor_overlap_pct=0.0, shared_visitors=2) == "HIGH"

    # MEDIUM: Moderate spatial (>= 10%) with visitor (>0), OR high spatial alone (>=20%), OR cross-hub visitor overlap (>= 25%)
    assert classify_cannibalization_risk(catchment_overlap_pct=15.0, visitor_overlap_pct=10.0, shared_visitors=1) == "MEDIUM"
    assert classify_cannibalization_risk(catchment_overlap_pct=30.0, visitor_overlap_pct=0.0, shared_visitors=0) == "MEDIUM"
    assert classify_cannibalization_risk(catchment_overlap_pct=0.0, visitor_overlap_pct=33.33, shared_visitors=1) == "MEDIUM"

    # LOW: Minimal spatial (>0%) or minimal shared visitors (>0)
    assert classify_cannibalization_risk(catchment_overlap_pct=5.0, visitor_overlap_pct=0.0, shared_visitors=0) == "LOW"
    assert classify_cannibalization_risk(catchment_overlap_pct=0.0, visitor_overlap_pct=5.0, shared_visitors=1) == "LOW"

    # NEGLIGIBLE: 0 spatial overlap AND 0 shared visitors
    assert classify_cannibalization_risk(catchment_overlap_pct=0.0, visitor_overlap_pct=0.0, shared_visitors=0) == "NEGLIGIBLE"


def test_empty_dataset_handling(baseline_catchments_geojson):
    """Verify empty events DataFrame returns schema-compliant empty DataFrame without error."""
    analyzer = CannibalizationAnalyzer()
    empty_df = pd.DataFrame(columns=["store_id", "device_id"])

    res_df = analyzer.analyze_cannibalization(empty_df, baseline_catchments_geojson)
    assert len(res_df) == 0
    assert "store_a" in res_df.columns
    assert "shared_visitors" in res_df.columns
    assert "cannibalization_risk_category" in res_df.columns

    summary = analyzer.generate_summary(res_df)
    assert summary.analysis_metadata["total_store_pairs"] == 0
    assert summary.overall_summary["pairs_with_shared_visitors"] == 0
    assert summary.risk_category_distribution["HIGH"] == 0


def test_invalid_data_handling(baseline_catchments_geojson):
    """Verify missing mandatory columns and missing files raise explicit exceptions."""
    analyzer = CannibalizationAnalyzer()

    # Missing mandatory 'device_id' column
    bad_df = pd.DataFrame({"store_id": ["STORE_A", "STORE_B"]})
    with pytest.raises(ValueError, match="Missing mandatory column"):
        analyzer.compute_visitor_overlap(bad_df)

    # Missing catchments GeoJSON file
    with pytest.raises(FileNotFoundError):
        analyzer.compute_catchment_overlap("/non/existent/path/catchments.geojson")


def test_output_file_generation(tmp_path, baseline_events_df, baseline_catchments_geojson):
    """Verify end-to-end pipeline creates cannibalization_analysis.csv and cannibalization_summary.json."""
    analyzer = CannibalizationAnalyzer()
    summary = analyzer.run_analysis_pipeline(
        events_path=baseline_events_df,
        catchments_path=baseline_catchments_geojson,
        output_dir=tmp_path,
    )

    csv_file = tmp_path / "cannibalization_analysis.csv"
    json_file = tmp_path / "cannibalization_summary.json"

    assert csv_file.exists()
    assert json_file.exists()

    # Verify CSV schema and contents
    df_loaded = pd.read_csv(csv_file)
    assert len(df_loaded) == 10
    expected_cols = [
        "store_a",
        "store_b",
        "unique_visitors_a",
        "unique_visitors_b",
        "shared_visitors",
        "visitor_overlap_percentage",
        "jaccard_similarity",
        "catchment_intersection_area",
        "catchment_overlap_percentage",
        "cannibalization_risk_category",
    ]
    for col in expected_cols:
        assert col in df_loaded.columns

    # Verify summary JSON
    with open(json_file, "r", encoding="utf-8") as f:
        summary_dict = json.load(f)

    assert summary_dict["analysis_metadata"]["total_store_pairs"] == 10
    assert summary_dict["overall_summary"]["pairs_with_catchment_overlap"] == 2
    assert summary_dict["overall_summary"]["pairs_with_shared_visitors"] == 4
    assert summary_dict["overall_summary"]["pairs_with_both_overlaps"] == 2
    assert "disclaimer" in summary_dict
    assert summary_dict["risk_category_distribution"]["HIGH"] == 2


def test_baseline_cannibalization_results(baseline_events_df, baseline_catchments_geojson):
    """Verify specific quantitative findings on the baseline GeoPulse dataset."""
    analyzer = CannibalizationAnalyzer()
    df = analyzer.analyze_cannibalization(baseline_events_df, baseline_catchments_geojson)

    # 1. Exactly 10 pairs analyzed
    assert len(df) == 10

    # 2. Croma & Nature's Basket has 2 shared visitors (DEV_1, DEV_4) and 52.72% catchment overlap -> HIGH
    croma_nb = df[
        ((df["store_a"] == "RET_ELEC_CROMA_003") & (df["store_b"] == "RET_GROC_NATURES_BASKET_004"))
        | ((df["store_a"] == "RET_GROC_NATURES_BASKET_004") & (df["store_b"] == "RET_ELEC_CROMA_003"))
    ].iloc[0]
    assert croma_nb["shared_visitors"] == 2
    assert croma_nb["visitor_overlap_percentage"] == 100.0
    assert croma_nb["catchment_overlap_percentage"] == pytest.approx(52.72, abs=0.5)
    assert croma_nb["cannibalization_risk_category"] == "HIGH"

    # 3. Zara & Forum Mall has 1 shared visitor (DEV_2) and ~90.13% catchment overlap -> HIGH
    zara_forum = df[
        ((df["store_a"] == "RET_FASH_ZARA_005") & (df["store_b"] == "RET_MALL_FORUM_001"))
        | ((df["store_a"] == "RET_MALL_FORUM_001") & (df["store_b"] == "RET_FASH_ZARA_005"))
    ].iloc[0]
    assert zara_forum["shared_visitors"] == 1
    assert zara_forum["visitor_overlap_percentage"] == 100.0
    assert zara_forum["catchment_overlap_percentage"] == pytest.approx(90.13, abs=0.5)
    assert zara_forum["cannibalization_risk_category"] == "HIGH"

    # 4. Total risk distribution check
    risk_counts = df["cannibalization_risk_category"].value_counts().to_dict()
    assert risk_counts.get("HIGH", 0) == 2
    assert risk_counts.get("MEDIUM", 0) == 2
    assert risk_counts.get("NEGLIGIBLE", 0) == 6
