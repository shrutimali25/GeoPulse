"""Retail POI (Points of Interest) catalog manager.

Loads, manages, and provides retail store coordinates and boundary definitions.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
import json

from src.common.config_loader import get_project_root, load_yaml_config
from src.common.logger import get_logger

logger = get_logger(__name__)


@dataclass
class RetailStore:
    """Represents a retail store point of interest."""

    store_id: str
    name: str
    category: str
    latitude: float
    longitude: float
    polygon_buffer_meters: float


class POICatalog:
    """Catalog holding all retail stores and zones for spatial mobility analysis."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or load_yaml_config()
        self.stores: List[RetailStore] = []
        self._load_stores_from_config()

    def _load_stores_from_config(self) -> None:
        """Parse retail store definitions from configuration."""
        raw_stores = self.config.get("retail_stores", [])
        for entry in raw_stores:
            self.stores.append(
                RetailStore(
                    store_id=entry["store_id"],
                    name=entry["name"],
                    category=entry["category"],
                    latitude=float(entry["latitude"]),
                    longitude=float(entry["longitude"]),
                    polygon_buffer_meters=float(entry.get("polygon_buffer_meters", 30.0)),
                )
            )
        logger.info(f"Loaded {len(self.stores)} retail stores into POI catalog.")

    def get_all_stores(self) -> List[RetailStore]:
        """Return all loaded retail stores."""
        return self.stores

    def get_store_by_id(self, store_id: str) -> Optional[RetailStore]:
        """Retrieve store by unique store_id."""
        for store in self.stores:
            if store.store_id == store_id:
                return store
        return None

    def load_geojson_features(self, filepath: Optional[Path] = None) -> Dict[str, Any]:
        """Load GeoJSON polygon features for store footprints.

        Args:
            filepath: Path to the GeoJSON file. Defaults to data/spatial/mall_boundaries.geojson.

        Returns:
            GeoJSON FeatureCollection dict.
        """
        if filepath is None:
            filepath = get_project_root() / "data" / "spatial" / "mall_boundaries.geojson"

        if not filepath.exists():
            raise FileNotFoundError(f"GeoJSON not found at: {filepath}")

        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
