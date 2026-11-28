"""Cannibalization & Visitor Overlap Analysis Engine for GeoPulse.

Computes pairwise store competition, footfall overlap, and spatial catchment interaction:
1. Visitor Overlap Analysis: Unique customer device counts, shared visitors, overlap percentage,
   and Jaccard similarity across store pairs.
2. Geographic Catchment Overlap: Intersection polygon computation, metric overlap area,
   and spatial overlap ratio relative to smaller catchment area in projected CRS (EPSG:32643).
3. Cannibalization Risk Categorization: Multi-dimensional risk scoring combining geographic
   proximity and observed customer co-visitation patterns.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import itertools
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = Any  # type: ignore

try:
    import geopandas as gpd
    from shapely.geometry import Polygon
    GEOPANDAS_AVAILABLE = True
except ImportError:
    GEOPANDAS_AVAILABLE = False
    gpd = Any  # type: ignore
    Polygon = Any  # type: ignore

from src.common.config_loader import get_project_root, load_yaml_config
from src.common.logger import get_logger

logger = get_logger(__name__)

DEFAULT_PROJECTED_CRS = "EPSG:32643"  # UTM Zone 43N (metric)

# Mandatory columns for visit events input
REQUIRED_EVENT_COLUMNS = ["store_id", "device_id"]


@dataclass
class CannibalizationPairResult:
    """Pairwise metrics for visitor and catchment overlap between two stores."""

    store_a: str
    store_b: str
    unique_visitors_a: int
    unique_visitors_b: int
    shared_visitors: int
    visitor_overlap_percentage: float
    jaccard_similarity: float
    catchment_intersection_area: float
    catchment_overlap_percentage: float
    cannibalization_risk_category: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert result dataclass to dictionary."""
        return asdict(self)


@dataclass
class CannibalizationSummary:
    """Statistical summary of cannibalization and visitor overlap analysis."""

    analysis_metadata: Dict[str, Any]
    overall_summary: Dict[str, Any]
    risk_category_distribution: Dict[str, int]
    highest_visitor_overlap_pairs: List[Dict[str, Any]]
    highest_catchment_overlap_pairs: List[Dict[str, Any]]
    disclaimer: str
    output_files: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert summary dataclass to dictionary."""
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        """Serialize summary to formatted JSON string."""
        return json.dumps(self.to_dict(), indent=indent)


def classify_cannibalization_risk(
    catchment_overlap_pct: float,
    visitor_overlap_pct: float,
    shared_visitors: int,
) -> str:
    """Classify cannibalization risk into HIGH, MEDIUM, LOW, or NEGLIGIBLE.

    Evaluation criteria:
    - HIGH: Substantial spatial catchment overlap (>= 20%) AND significant visitor overlap
      (>= 25% or >= 2 shared customer devices).
    - MEDIUM: Moderate catchment overlap (>= 10%) with observed co-visitation, OR high
      catchment overlap without observed visits, OR notable cross-shopping overlap (>= 25%).
    - LOW: Minimal spatial interaction (> 0%) or minimal co-visitation (> 0 shared devices).
    - NEGLIGIBLE: Zero spatial catchment overlap AND zero shared customer devices.

    Args:
        catchment_overlap_pct: Percentage of smaller catchment area overlapping.
        visitor_overlap_pct: Percentage of smaller visitor base shared.
        shared_visitors: Count of unique devices visiting both stores.

    Returns:
        Risk classification string ("HIGH", "MEDIUM", "LOW", or "NEGLIGIBLE").
    """
    if catchment_overlap_pct >= 20.0 and (visitor_overlap_pct >= 25.0 or shared_visitors >= 2):
        return "HIGH"
    if (
        (catchment_overlap_pct >= 10.0 and shared_visitors >= 1)
        or (catchment_overlap_pct >= 20.0)
        or (visitor_overlap_pct >= 25.0)
    ):
        return "MEDIUM"
    if catchment_overlap_pct > 0.0 or shared_visitors > 0:
        return "LOW"
    return "NEGLIGIBLE"


class CannibalizationAnalyzer:
    """Analyzes customer visitor overlap and geographic catchment intersection across store pairs."""

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        projected_crs: str = DEFAULT_PROJECTED_CRS,
    ):
        """Initialize cannibalization analyzer.

        Args:
            config: Optional configuration dictionary. Defaults to base.yaml.
            projected_crs: Metric CRS for accurate surface area calculations (default EPSG:32643).
        """
        self.config = config or load_yaml_config()
        self.projected_crs = projected_crs

    # -------------------------------------------------------------------------
    # A. Visitor Overlap Analysis
    # -------------------------------------------------------------------------
    def compute_visitor_overlap(
        self,
        events_df: Any,
    ) -> Dict[Tuple[str, str], Dict[str, Any]]:
        """Compute unique visitors, shared visitors, overlap ratio, and Jaccard similarity.

        Args:
            events_df: DataFrame containing at least 'store_id' and 'device_id'.

        Returns:
            Dictionary mapping store pairs (store_a, store_b) to overlap metrics.
        """
        if events_df is None or len(events_df) == 0:
            return {}

        # Validate mandatory columns
        for col in REQUIRED_EVENT_COLUMNS:
            if col not in events_df.columns:
                raise ValueError(f"Missing mandatory column '{col}' for visitor overlap analysis.")

        # Extract unique visitor set per store
        store_visitors: Dict[str, Set[str]] = {}
        for store_id, group in events_df.groupby("store_id"):
            devices = set(group["device_id"].dropna().astype(str))
            store_visitors[str(store_id)] = devices

        store_ids = sorted(list(store_visitors.keys()))
        overlap_results: Dict[Tuple[str, str], Dict[str, Any]] = {}

        for store_a, store_b in itertools.combinations(store_ids, 2):
            visitors_a = store_visitors[store_a]
            visitors_b = store_visitors[store_b]

            count_a = len(visitors_a)
            count_b = len(visitors_b)
            shared = len(visitors_a.intersection(visitors_b))
            union = len(visitors_a.union(visitors_b))

            min_visitors = min(count_a, count_b)
            overlap_pct = round((shared / min_visitors * 100.0), 2) if min_visitors > 0 else 0.0
            jaccard = round((shared / union), 4) if union > 0 else 0.0

            overlap_results[(store_a, store_b)] = {
                "unique_visitors_a": count_a,
                "unique_visitors_b": count_b,
                "shared_visitors": shared,
                "visitor_overlap_percentage": overlap_pct,
                "jaccard_similarity": jaccard,
            }

        return overlap_results

    # -------------------------------------------------------------------------
    # B. Geographic Catchment Overlap Analysis
    # -------------------------------------------------------------------------
    def compute_catchment_overlap(
        self,
        catchments_data: Union[str, Path, Any],
    ) -> Dict[Tuple[str, str], Dict[str, Any]]:
        """Calculate pairwise polygon intersection area and overlap percentage in metric CRS.

        Args:
            catchments_data: Path to store catchments GeoJSON, or GeoDataFrame.

        Returns:
            Dictionary mapping store pairs (store_a, store_b) to spatial overlap metrics.
        """
        if not GEOPANDAS_AVAILABLE:
            raise ImportError("geopandas and shapely are required for spatial catchment overlap.")

        if isinstance(catchments_data, (str, Path)):
            p = Path(catchments_data)
            if not p.exists():
                raise FileNotFoundError(f"Catchments GeoJSON file not found: {p}")
            gdf = gpd.read_file(p)
        elif hasattr(catchments_data, "geometry"):
            gdf = catchments_data.copy()
        else:
            raise ValueError("catchments_data must be a valid file path or GeoDataFrame.")

        if len(gdf) == 0:
            return {}

        if "store_id" not in gdf.columns:
            raise ValueError("Catchments GeoDataFrame missing mandatory 'store_id' column.")

        # Ensure projected metric CRS for accurate area in square meters
        if gdf.crs is None or gdf.crs.to_string() != self.projected_crs:
            logger.debug(f"Reprojecting catchments to {self.projected_crs} for metric area computation.")
            gdf_proj = gdf.to_crs(self.projected_crs)
        else:
            gdf_proj = gdf.copy()

        store_geoms: Dict[str, Any] = {}
        for _, row in gdf_proj.iterrows():
            store_geoms[str(row["store_id"])] = row.geometry

        store_ids = sorted(list(store_geoms.keys()))
        spatial_results: Dict[Tuple[str, str], Dict[str, Any]] = {}

        for store_a, store_b in itertools.combinations(store_ids, 2):
            geom_a = store_geoms[store_a]
            geom_b = store_geoms[store_b]

            area_a = geom_a.area if geom_a is not None else 0.0
            area_b = geom_b.area if geom_b is not None else 0.0

            intersection_area = 0.0
            overlap_pct = 0.0

            if geom_a is not None and geom_b is not None and geom_a.intersects(geom_b):
                inter_geom = geom_a.intersection(geom_b)
                intersection_area = round(float(inter_geom.area), 2)
                min_area = min(area_a, area_b)
                if min_area > 0:
                    overlap_pct = round((intersection_area / min_area * 100.0), 2)

            spatial_results[(store_a, store_b)] = {
                "catchment_intersection_area": intersection_area,
                "catchment_overlap_percentage": overlap_pct,
            }

        return spatial_results

    # -------------------------------------------------------------------------
    # C. Pairwise Cannibalization Analysis & Dataframe Construction
    # -------------------------------------------------------------------------
    def analyze_cannibalization(
        self,
        events_df: Any,
        catchments_data: Union[str, Path, Any],
    ) -> pd.DataFrame:
        """Combine visitor co-visitation and geographic catchment overlap for all store pairs.

        Args:
            events_df: DataFrame of detected visit events.
            catchments_data: Path to store catchments GeoJSON, or GeoDataFrame.

        Returns:
            DataFrame containing complete pairwise cannibalization metrics.
        """
        cols = [
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
        if events_df is None or len(events_df) == 0:
            return pd.DataFrame(columns=cols)

        visitor_metrics = self.compute_visitor_overlap(events_df)
        spatial_metrics = self.compute_catchment_overlap(catchments_data)

        # Collect union of all identified store pairs
        all_pairs = sorted(list(set(visitor_metrics.keys()).union(set(spatial_metrics.keys()))))
        if not all_pairs:
            return pd.DataFrame(columns=cols)

        records: List[Dict[str, Any]] = []
        for pair in all_pairs:
            store_a, store_b = pair
            v_data = visitor_metrics.get(pair, {
                "unique_visitors_a": 0,
                "unique_visitors_b": 0,
                "shared_visitors": 0,
                "visitor_overlap_percentage": 0.0,
                "jaccard_similarity": 0.0,
            })
            s_data = spatial_metrics.get(pair, {
                "catchment_intersection_area": 0.0,
                "catchment_overlap_percentage": 0.0,
            })

            risk_cat = classify_cannibalization_risk(
                catchment_overlap_pct=s_data["catchment_overlap_percentage"],
                visitor_overlap_pct=v_data["visitor_overlap_percentage"],
                shared_visitors=v_data["shared_visitors"],
            )

            record = CannibalizationPairResult(
                store_a=store_a,
                store_b=store_b,
                unique_visitors_a=v_data["unique_visitors_a"],
                unique_visitors_b=v_data["unique_visitors_b"],
                shared_visitors=v_data["shared_visitors"],
                visitor_overlap_percentage=v_data["visitor_overlap_percentage"],
                jaccard_similarity=v_data["jaccard_similarity"],
                catchment_intersection_area=s_data["catchment_intersection_area"],
                catchment_overlap_percentage=s_data["catchment_overlap_percentage"],
                cannibalization_risk_category=risk_cat,
            )
            records.append(record.to_dict())

        df_result = pd.DataFrame(records)
        # Sort by shared visitors desc, catchment overlap desc, then store_a
        df_result = df_result.sort_values(
            by=["shared_visitors", "catchment_overlap_percentage", "store_a"],
            ascending=[False, False, True],
        ).reset_index(drop=True)
        return df_result[cols]

    # -------------------------------------------------------------------------
    # D. Executive Summary Generation
    # -------------------------------------------------------------------------
    def generate_summary(
        self,
        analysis_df: pd.DataFrame,
        events_path: Optional[str] = None,
        catchments_path: Optional[str] = None,
        output_files: Optional[Dict[str, str]] = None,
    ) -> CannibalizationSummary:
        """Generate structured executive summary JSON for cannibalization analysis.

        Args:
            analysis_df: Output DataFrame from analyze_cannibalization.
            events_path: Path or description of events data source.
            catchments_path: Path or description of catchments data source.
            output_files: Dictionary of generated deliverable paths.

        Returns:
            CannibalizationSummary instance.
        """
        now_utc = datetime.now(timezone.utc).isoformat()
        disclaimer = (
            "This analysis measures geographic catchment overlap and observed shared visitor footfall. "
            "It serves as an indicator of spatial proximity and customer co-visitation, NOT definitive "
            "proof of sales cannibalization, as transaction and revenue figures are not present in GPS telemetry."
        )

        if analysis_df is None or len(analysis_df) == 0:
            return CannibalizationSummary(
                analysis_metadata={
                    "generated_at_utc": now_utc,
                    "source_events_file": events_path or "",
                    "source_catchments_file": catchments_path or "",
                    "projected_crs": self.projected_crs,
                    "total_store_pairs": 0,
                    "status": "EMPTY_DATASET",
                },
                overall_summary={
                    "pairs_with_catchment_overlap": 0,
                    "pairs_with_shared_visitors": 0,
                    "pairs_with_both_overlaps": 0,
                    "max_catchment_overlap_pct": 0.0,
                    "max_visitor_overlap_pct": 0.0,
                    "max_shared_visitors": 0,
                },
                risk_category_distribution={
                    "HIGH": 0,
                    "MEDIUM": 0,
                    "LOW": 0,
                    "NEGLIGIBLE": 0,
                },
                highest_visitor_overlap_pairs=[],
                highest_catchment_overlap_pairs=[],
                disclaimer=disclaimer,
                output_files=output_files or {},
            )

        total_pairs = len(analysis_df)
        spatial_overlap_pairs = int((analysis_df["catchment_overlap_percentage"] > 0).sum())
        visitor_overlap_pairs = int((analysis_df["shared_visitors"] > 0).sum())
        both_overlap_pairs = int(
            ((analysis_df["catchment_overlap_percentage"] > 0) & (analysis_df["shared_visitors"] > 0)).sum()
        )

        max_spatial_pct = float(analysis_df["catchment_overlap_percentage"].max())
        max_visitor_pct = float(analysis_df["visitor_overlap_percentage"].max())
        max_shared_v = int(analysis_df["shared_visitors"].max())

        risk_counts = analysis_df["cannibalization_risk_category"].value_counts().to_dict()
        risk_dist = {
            "HIGH": int(risk_counts.get("HIGH", 0)),
            "MEDIUM": int(risk_counts.get("MEDIUM", 0)),
            "LOW": int(risk_counts.get("LOW", 0)),
            "NEGLIGIBLE": int(risk_counts.get("NEGLIGIBLE", 0)),
        }

        # Top visitor overlap pairs
        top_v = analysis_df.sort_values(
            by=["shared_visitors", "visitor_overlap_percentage"], ascending=[False, False]
        ).head(5)
        top_visitor_list = [
            {
                "store_a": str(r["store_a"]),
                "store_b": str(r["store_b"]),
                "shared_visitors": int(r["shared_visitors"]),
                "visitor_overlap_percentage": float(r["visitor_overlap_percentage"]),
                "jaccard_similarity": float(r["jaccard_similarity"]),
                "catchment_overlap_percentage": float(r["catchment_overlap_percentage"]),
                "cannibalization_risk_category": str(r["cannibalization_risk_category"]),
            }
            for _, r in top_v.iterrows()
            if r["shared_visitors"] > 0
        ]

        # Top geographic catchment overlap pairs
        top_s = analysis_df.sort_values(
            by=["catchment_overlap_percentage", "catchment_intersection_area"], ascending=[False, False]
        ).head(5)
        top_catchment_list = [
            {
                "store_a": str(r["store_a"]),
                "store_b": str(r["store_b"]),
                "catchment_intersection_area_sqm": float(r["catchment_intersection_area"]),
                "catchment_overlap_percentage": float(r["catchment_overlap_percentage"]),
                "shared_visitors": int(r["shared_visitors"]),
                "visitor_overlap_percentage": float(r["visitor_overlap_percentage"]),
                "cannibalization_risk_category": str(r["cannibalization_risk_category"]),
            }
            for _, r in top_s.iterrows()
            if r["catchment_overlap_percentage"] > 0
        ]

        return CannibalizationSummary(
            analysis_metadata={
                "generated_at_utc": now_utc,
                "source_events_file": events_path or "",
                "source_catchments_file": catchments_path or "",
                "projected_crs": self.projected_crs,
                "total_store_pairs": total_pairs,
                "status": "COMPLETED",
            },
            overall_summary={
                "pairs_with_catchment_overlap": spatial_overlap_pairs,
                "pairs_with_shared_visitors": visitor_overlap_pairs,
                "pairs_with_both_overlaps": both_overlap_pairs,
                "max_catchment_overlap_pct": max_spatial_pct,
                "max_visitor_overlap_pct": max_visitor_pct,
                "max_shared_visitors": max_shared_v,
            },
            risk_category_distribution=risk_dist,
            highest_visitor_overlap_pairs=top_visitor_list,
            highest_catchment_overlap_pairs=top_catchment_list,
            disclaimer=disclaimer,
            output_files=output_files or {},
        )

    # -------------------------------------------------------------------------
    # E. End-to-End Execution Pipeline
    # -------------------------------------------------------------------------
    def run_analysis_pipeline(
        self,
        events_path: Optional[Union[str, Path, Any]] = None,
        catchments_path: Optional[Union[str, Path, Any]] = None,
        output_dir: Optional[Union[str, Path]] = None,
    ) -> CannibalizationSummary:
        """Execute complete cannibalization and visitor overlap analysis pipeline.

        1. Ingests visit events and store catchments.
        2. Computes pairwise visitor overlap, Jaccard similarity, and catchment intersection.
        3. Assigns cannibalization risk categories.
        4. Saves cannibalization_analysis.csv and cannibalization_summary.json to output_dir.

        Args:
            events_path: Path to store_catchment_events.csv, or DataFrame.
            catchments_path: Path to store_catchments_500m.geojson, or GeoDataFrame.
            output_dir: Destination directory (defaults to data/processed/analytics).

        Returns:
            CannibalizationSummary detailing findings and generated file paths.
        """
        root = get_project_root()

        # Resolve events source
        if events_path is None:
            csv_default = root / "data" / "processed" / "store_catchment_events.csv"
            if not csv_default.exists():
                raise FileNotFoundError(f"Default events CSV missing: {csv_default}")
            events_src: Any = csv_default
        else:
            events_src = events_path

        # Resolve catchments source
        if catchments_path is None:
            geojson_default = root / "data" / "spatial" / "store_catchments_500m.geojson"
            if not geojson_default.exists():
                raise FileNotFoundError(f"Default catchments GeoJSON missing: {geojson_default}")
            catchments_src: Any = geojson_default
        else:
            catchments_src = catchments_path

        # Resolve output directory
        target_dir = Path(output_dir) if output_dir else root / "data" / "processed" / "analytics"
        target_dir.mkdir(parents=True, exist_ok=True)

        # Load events if path
        if isinstance(events_src, (str, Path)):
            df_events = pd.read_csv(events_src)
            events_label = str(events_src)
        else:
            df_events = events_src
            events_label = "DataFrame"

        catchments_label = str(catchments_src) if isinstance(catchments_src, (str, Path)) else "GeoDataFrame"

        # Execute analysis
        logger.info("Executing pairwise cannibalization and visitor overlap calculations...")
        analysis_df = self.analyze_cannibalization(
            events_df=df_events,
            catchments_data=catchments_src,
        )

        csv_path = target_dir / "cannibalization_analysis.csv"
        json_path = target_dir / "cannibalization_summary.json"

        # Save CSV deliverable
        analysis_df.to_csv(csv_path, index=False)

        output_files = {
            "cannibalization_analysis_csv": str(csv_path),
            "cannibalization_summary_json": str(json_path),
        }

        # Generate and save summary JSON
        summary = self.generate_summary(
            analysis_df=analysis_df,
            events_path=events_label,
            catchments_path=catchments_label,
            output_files=output_files,
        )

        with open(json_path, "w", encoding="utf-8") as f:
            f.write(summary.to_json(indent=2))

        logger.info(
            f"Cannibalization analysis complete: {len(analysis_df)} store pairs analyzed. "
            f"Saved to {csv_path} and {json_path}."
        )
        return summary


if __name__ == "__main__":
    analyzer = CannibalizationAnalyzer()
    summary = analyzer.run_analysis_pipeline()
    print("\n" + "=" * 75)
    print(" GeoPulse Day 9: Cannibalization & Visitor Overlap Analysis Summary")
    print("=" * 75)
    print(summary.to_json(indent=2))
