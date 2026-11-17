"""GPS Trajectory Simulation Engine for Retail Mobility Analytics.

Generates realistic, synthetic spatio-temporal mobility traces within a configured
study zone (e.g. Koramangala, Bengaluru) for testing and pipeline development.

NOTE: This generator produces purely SYNTHETIC telemetry using kinematic,
stochastic (Markov/random walk), and POI attraction models. It does NOT simulate
actual individual human tracking or real-world road networks. All records are
explicitly flagged with `is_synthetic = True`.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
import math
import os
from pathlib import Path
import random
from typing import Any, Dict, List, Optional, Tuple
import uuid

from src.common.config_loader import get_project_root, load_yaml_config
from src.common.logger import get_logger
from src.member1_data_eng.generator.poi_catalog import POICatalog, RetailStore

logger = get_logger(__name__)

# Constants for coordinate transformations near Koramangala (Lat ~12.93° N)
METERS_PER_DEG_LAT = 110574.0
METERS_PER_DEG_LON = 108584.0

# Optional PyArrow support
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False


@dataclass
class TrajectoryConfig:
    """Simulation run parameters."""

    device_count: int = 500
    duration_hours: int = 24
    interval_seconds: int = 30
    min_lat: float = 12.9200
    max_lat: float = 12.9450
    min_lon: float = 77.6100
    max_lon: float = 77.6400
    noise_std_meters: float = 4.5
    category_ratios: Dict[str, float] = field(
        default_factory=lambda: {
            "daily_shoppers": 0.40,
            "office_commuters": 0.35,
            "transient_pedestrians": 0.25,
        }
    )
    random_seed: int = 42
    multipath_probability: float = 0.05
    multipath_max_offset_meters: float = 15.0
    peak_hours: List[int] = field(default_factory=lambda: [12, 13, 18, 19, 20, 21])


class TrajectorySimulator:
    """Simulates synthetic mobile device GPS trajectories across a defined study zone.

    Features:
    - Deterministic seeding for fully reproducible generation
    - Three distinct mobility profiles: daily shoppers, office commuters, and transient pedestrians
    - Dwell mechanics around retail store POIs
    - Gaussian GPS observation noise and occasional multipath spikes
    - Bounded coordinate enforcement inside the configured study zone
    - Memory-efficient batch writing (JSONL streaming or Parquet if pyarrow is installed)
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        poi_catalog: Optional[POICatalog] = None,
        seed: Optional[int] = None,
    ):
        self.raw_config = config or load_yaml_config()
        self.poi_catalog = poi_catalog or POICatalog(self.raw_config)
        self.sim_config = self._init_trajectory_config()
        if seed is not None:
            self.sim_config.random_seed = seed

        logger.info(
            f"Initialized TrajectorySimulator: {self.sim_config.device_count} devices, "
            f"{self.sim_config.duration_hours}h duration, seed={self.sim_config.random_seed}."
        )

    def _init_trajectory_config(self) -> TrajectoryConfig:
        """Extract simulation parameters from YAML config."""
        sim_params = self.raw_config.get("telemetry_simulation", {})
        study_zone = self.raw_config.get("study_zone", {}).get("bounding_box", {})
        devices_cfg = sim_params.get("devices", {})
        temporal_cfg = sim_params.get("temporal", {})
        noise_cfg = sim_params.get("spatial_noise", {})

        return TrajectoryConfig(
            device_count=devices_cfg.get("total_count", 500),
            duration_hours=temporal_cfg.get("default_duration_hours", 24),
            interval_seconds=temporal_cfg.get("sampling_interval_seconds", 30),
            min_lat=float(study_zone.get("min_lat", 12.9200)),
            max_lat=float(study_zone.get("max_lat", 12.9450)),
            min_lon=float(study_zone.get("min_lon", 77.6100)),
            max_lon=float(study_zone.get("max_lon", 77.6400)),
            noise_std_meters=float(noise_cfg.get("gps_drift_std_meters", 4.5)),
            category_ratios=devices_cfg.get(
                "categories",
                {
                    "daily_shoppers": 0.40,
                    "office_commuters": 0.35,
                    "transient_pedestrians": 0.25,
                },
            ),
            random_seed=int(sim_params.get("random_seed", 42)),
            multipath_probability=float(noise_cfg.get("multipath_error_probability", 0.05)),
            multipath_max_offset_meters=float(noise_cfg.get("multipath_max_offset_meters", 15.0)),
            peak_hours=temporal_cfg.get("peak_hours", [12, 13, 18, 19, 20, 21]),
        )

    def _get_device_rng(self, device_id: str, seed_override: Optional[int] = None) -> random.Random:
        """Create a deterministic Random instance for an individual device."""
        base_seed = seed_override if seed_override is not None else self.sim_config.random_seed
        # Compute deterministic integer hash from device_id
        device_int = int(device_id.replace("DEV_", ""), 16) if "_" in device_id else hash(device_id)
        combined_seed = (base_seed * 10007 + device_int * 37) & 0xFFFFFFFF
        return random.Random(combined_seed)

    def _clamp_coordinates(self, lat: float, lon: float) -> Tuple[float, float]:
        """Ensure coordinates stay strictly within study zone bounding box."""
        clamped_lat = max(self.sim_config.min_lat, min(lat, self.sim_config.max_lat))
        clamped_lon = max(self.sim_config.min_lon, min(lon, self.sim_config.max_lon))
        return clamped_lat, clamped_lon

    def generate_single_device_trajectory(
        self,
        device_id: str,
        start_time: datetime,
        profile_type: str = "daily_shoppers",
        duration_hours: Optional[int] = None,
        interval_seconds: Optional[int] = None,
        seed: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Simulate realistic movement for a single device over the configured duration.

        Args:
            device_id: Unique device identifier (e.g. 'DEV_00000001').
            start_time: Start timestamp in UTC.
            profile_type: 'daily_shoppers', 'office_commuters', or 'transient_pedestrians'.
            duration_hours: Total duration in hours (defaults to config).
            interval_seconds: Ping interval in seconds (defaults to config).
            seed: Optional seed override for this trajectory.

        Returns:
            List of validated dictionary records representing sequential GPS pings.
        """
        rng = self._get_device_rng(device_id, seed)
        duration = duration_hours if duration_hours is not None else self.sim_config.duration_hours
        interval = interval_seconds if interval_seconds is not None else self.sim_config.interval_seconds

        total_steps = max(1, int((duration * 3600) / interval))
        if start_time.tzinfo is None:
            current_time = start_time.replace(tzinfo=timezone.utc)
        else:
            current_time = start_time.astimezone(timezone.utc)

        # Initialize starting location within the study zone
        lat = rng.uniform(
            self.sim_config.min_lat + 0.002, self.sim_config.max_lat - 0.002
        )
        lon = rng.uniform(
            self.sim_config.min_lon + 0.002, self.sim_config.max_lon - 0.002
        )
        heading = rng.uniform(0.0, 360.0)

        # Load available stores for attraction/dwell
        stores = self.poi_catalog.get_all_stores()
        target_store: Optional[RetailStore] = None
        dwell_steps_remaining = 0

        if stores and profile_type in ("daily_shoppers", "office_commuters"):
            target_store = rng.choice(stores)

        pings: List[Dict[str, Any]] = []
        prev_recorded_lat, prev_recorded_lon = lat, lon

        for step in range(total_steps):
            ping_time = current_time + timedelta(seconds=step * interval)
            hour_of_day = ping_time.hour

            # ---------------------------------------------------------
            # 1. State & Kinematic Update based on Profile
            # ---------------------------------------------------------
            if profile_type == "daily_shoppers":
                # Daily shopper: travels to retail store, dwells, moves to next
                if dwell_steps_remaining > 0:
                    # Currently dwelling inside store
                    speed = rng.uniform(0.05, 0.45)
                    heading = (heading + rng.uniform(-30, 30)) % 360.0
                    dwell_steps_remaining -= 1
                    if dwell_steps_remaining == 0 and stores:
                        # Switch to another store
                        target_store = rng.choice([s for s in stores if s != target_store] or stores)
                else:
                    # Walking towards target store
                    speed = rng.uniform(1.0, 1.6)  # 3.6 - 5.8 km/h
                    if target_store:
                        d_lat = (target_store.latitude - lat) * METERS_PER_DEG_LAT
                        d_lon = (target_store.longitude - lon) * METERS_PER_DEG_LON
                        dist_to_store = math.sqrt(d_lat * d_lat + d_lon * d_lon)

                        target_bearing = math.degrees(math.atan2(d_lon, d_lat)) % 360.0
                        heading = (target_bearing + rng.gauss(0, 15.0)) % 360.0

                        if dist_to_store <= target_store.polygon_buffer_meters:
                            # Arrived at store: begin dwell (20 - 60 minutes)
                            dwell_duration_sec = rng.uniform(20 * 60, 60 * 60)
                            dwell_steps_remaining = int(dwell_duration_sec / interval)
                    else:
                        heading = (heading + rng.gauss(0, 25.0)) % 360.0

            elif profile_type == "office_commuters":
                # Commuter: High vehicle speed during rush hours; stationary during work/night
                is_morning_rush = 8 <= hour_of_day < 10
                is_evening_rush = 17 <= hour_of_day < 20
                is_lunch_hour = 12 <= hour_of_day < 14

                if is_morning_rush or is_evening_rush:
                    # Vehicle transit
                    speed = rng.uniform(7.0, 14.0)  # 25 - 50 km/h
                    heading = (heading + rng.gauss(0, 8.0)) % 360.0
                elif is_lunch_hour:
                    # Midday cafe trip or short walk
                    if dwell_steps_remaining > 0:
                        speed = rng.uniform(0.05, 0.35)
                        dwell_steps_remaining -= 1
                    else:
                        speed = rng.uniform(0.9, 1.5)
                        if target_store:
                            d_lat = (target_store.latitude - lat) * METERS_PER_DEG_LAT
                            d_lon = (target_store.longitude - lon) * METERS_PER_DEG_LON
                            dist = math.sqrt(d_lat * d_lat + d_lon * d_lon)
                            if dist <= target_store.polygon_buffer_meters:
                                dwell_steps_remaining = int(rng.uniform(15 * 60, 35 * 60) / interval)
                            else:
                                target_bearing = math.degrees(math.atan2(d_lon, d_lat)) % 360.0
                                heading = (target_bearing + rng.gauss(0, 12.0)) % 360.0
                else:
                    # Stationary at office or home
                    speed = rng.uniform(0.0, 0.15)
                    heading = (heading + rng.uniform(-10, 10)) % 360.0

            else:
                # transient_pedestrians: Correlated random walk on sidewalks
                # Occasional short pauses (2 - 5 min window browsing)
                if rng.random() < 0.08 and dwell_steps_remaining == 0:
                    dwell_steps_remaining = int(rng.uniform(2 * 60, 5 * 60) / interval)

                if dwell_steps_remaining > 0:
                    speed = rng.uniform(0.0, 0.25)
                    dwell_steps_remaining -= 1
                else:
                    speed = rng.uniform(0.8, 1.5)
                    heading = (heading + rng.gauss(0, 22.0)) % 360.0

            # ---------------------------------------------------------
            # 2. Kinematic Position Advance & Bounding Reflection
            # ---------------------------------------------------------
            rad = math.radians(heading)
            delta_y = speed * interval * math.cos(rad)
            delta_x = speed * interval * math.sin(rad)

            lat += delta_y / METERS_PER_DEG_LAT
            lon += delta_x / METERS_PER_DEG_LON

            # Reflect off study-zone boundary cushions
            boundary_margin_deg = 0.0008  # ~90m cushion
            if lat < self.sim_config.min_lat + boundary_margin_deg:
                lat = self.sim_config.min_lat + boundary_margin_deg
                heading = (180.0 - heading) % 360.0
            elif lat > self.sim_config.max_lat - boundary_margin_deg:
                lat = self.sim_config.max_lat - boundary_margin_deg
                heading = (180.0 - heading) % 360.0

            if lon < self.sim_config.min_lon + boundary_margin_deg:
                lon = self.sim_config.min_lon + boundary_margin_deg
                heading = (360.0 - heading) % 360.0
            elif lon > self.sim_config.max_lon - boundary_margin_deg:
                lon = self.sim_config.max_lon - boundary_margin_deg
                heading = (360.0 - heading) % 360.0

            true_lat, true_lon = self._clamp_coordinates(lat, lon)

            # ---------------------------------------------------------
            # 3. GPS Observation Noise & Multipath Injection
            # ---------------------------------------------------------
            noise_x = rng.gauss(0.0, self.sim_config.noise_std_meters)
            noise_y = rng.gauss(0.0, self.sim_config.noise_std_meters)
            accuracy = round(rng.uniform(3.2, 5.8), 2)

            # Multipath anomaly check
            if rng.random() < self.sim_config.multipath_probability:
                mp_dist = rng.uniform(7.0, self.sim_config.multipath_max_offset_meters)
                mp_angle = rng.uniform(0.0, 2.0 * math.pi)
                noise_x += mp_dist * math.sin(mp_angle)
                noise_y += mp_dist * math.cos(mp_angle)
                accuracy = round(rng.uniform(12.0, 24.5), 2)

            recorded_lat = true_lat + (noise_y / METERS_PER_DEG_LAT)
            recorded_lon = true_lon + (noise_x / METERS_PER_DEG_LON)
            recorded_lat, recorded_lon = self._clamp_coordinates(recorded_lat, recorded_lon)

            # Compute observed speed from consecutive pings
            if step == 0:
                calc_speed = round(speed, 2)
            else:
                dist_m = math.sqrt(
                    ((recorded_lat - prev_recorded_lat) * METERS_PER_DEG_LAT) ** 2
                    + ((recorded_lon - prev_recorded_lon) * METERS_PER_DEG_LON) ** 2
                )
                calc_speed = round(min(dist_m / interval, 65.0), 2)

            prev_recorded_lat, prev_recorded_lon = recorded_lat, recorded_lon

            # Generate unique ping ID
            ping_id = str(uuid.UUID(bytes=rng.randbytes(16), version=4))
            altitude_m = round(920.0 + rng.gauss(0.0, 1.2), 1)  # Bengaluru elevation

            ping_record: Dict[str, Any] = {
                "ping_id": ping_id,
                "device_id": device_id,
                "timestamp_utc": ping_time,
                "latitude": round(recorded_lat, 6),
                "longitude": round(recorded_lon, 6),
                "altitude_meters": altitude_m,
                "horizontal_accuracy_meters": accuracy,
                "speed_mps": calc_speed,
                "heading_degrees": round(heading, 1),
                "is_synthetic": True,
                "profile_type": profile_type,
            }
            pings.append(ping_record)

        return pings

    def _assign_device_profiles(self, count: int) -> List[Tuple[str, str]]:
        """Assign device IDs and category profiles matching configured ratios.

        Returns:
            List of (device_id, profile_name) tuples.
        """
        ratios = self.sim_config.category_ratios
        shoppers_ratio = ratios.get("daily_shoppers", 0.40)
        commuters_ratio = ratios.get("office_commuters", 0.35)

        num_shoppers = int(round(count * shoppers_ratio))
        num_commuters = int(round(count * commuters_ratio))
        num_pedestrians = max(0, count - num_shoppers - num_commuters)

        assignments: List[Tuple[str, str]] = []
        idx = 1

        for _ in range(num_shoppers):
            assignments.append((f"DEV_{idx:08X}", "daily_shoppers"))
            idx += 1
        for _ in range(num_commuters):
            assignments.append((f"DEV_{idx:08X}", "office_commuters"))
            idx += 1
        for _ in range(num_pedestrians):
            assignments.append((f"DEV_{idx:08X}", "transient_pedestrians"))
            idx += 1

        return assignments

    def run_simulation(
        self,
        start_time: Optional[datetime] = None,
        device_count: Optional[int] = None,
        duration_hours: Optional[int] = None,
        output_dir: Optional[str] = None,
        output_format: str = "auto",
        batch_size: int = 50,
    ) -> Dict[str, Any]:
        """Execute batch simulation and write telemetry to disk in streaming chunks.

        Avoids holding the entire multi-device dataset in memory.

        Args:
            start_time: Simulation start time (UTC). Defaults to today 00:00:00 UTC.
            device_count: Optional override for device count.
            duration_hours: Optional override for duration in hours.
            output_dir: Target output directory. Defaults to 'data/raw/date=YYYY-MM-DD/'.
            output_format: 'auto', 'parquet', or 'jsonl'.
            batch_size: Number of devices to generate per disk write batch.

        Returns:
            Summary dict with generated metrics and output file locations.
        """
        total_devices = device_count if device_count is not None else self.sim_config.device_count
        duration = duration_hours if duration_hours is not None else self.sim_config.duration_hours

        if start_time is None:
            now = datetime.now(timezone.utc)
            start_time = datetime(now.year, now.month, now.day, 0, 0, 0, tzinfo=timezone.utc)
        elif start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)

        date_str = start_time.strftime("%Y-%m-%d")
        if output_dir:
            out_path = Path(output_dir)
        else:
            out_path = get_project_root() / "data" / "raw" / f"date={date_str}"

        out_path.mkdir(parents=True, exist_ok=True)

        use_parquet = PYARROW_AVAILABLE and (output_format in ("auto", "parquet"))
        if not PYARROW_AVAILABLE and output_format == "parquet":
            logger.warning(
                "pyarrow is not installed. Parquet format cannot be produced. "
                "Falling back to JSON Lines (.jsonl). To enable Parquet, install requirements-spatial.txt."
            )
            use_parquet = False

        device_assignments = self._assign_device_profiles(total_devices)
        total_pings = 0
        written_files: List[str] = []

        logger.info(
            f"Starting simulation: {total_devices} devices, {duration}h, format="
            f"{'parquet' if use_parquet else 'jsonl'}, destination={out_path}"
        )

        # Process in batches to keep memory usage low
        for batch_idx in range(0, total_devices, batch_size):
            batch_devices = device_assignments[batch_idx : batch_idx + batch_size]
            batch_pings: List[Dict[str, Any]] = []

            for dev_id, profile in batch_devices:
                dev_pings = self.generate_single_device_trajectory(
                    device_id=dev_id,
                    start_time=start_time,
                    profile_type=profile,
                    duration_hours=duration,
                    interval_seconds=self.sim_config.interval_seconds,
                )
                batch_pings.extend(dev_pings)

            part_num = (batch_idx // batch_size) + 1
            if use_parquet:
                part_file = out_path / f"telemetry_part_{part_num:03d}.parquet"
                # Prepare PyArrow table
                arrow_records = []
                for p in batch_pings:
                    rec_copy = dict(p)
                    # Convert datetime to timestamp for pyarrow
                    arrow_records.append(rec_copy)
                table = pa.Table.from_pylist(arrow_records)
                pq.write_table(table, str(part_file), compression="snappy")
                written_files.append(str(part_file))
            else:
                part_file = out_path / f"telemetry_part_{part_num:03d}.jsonl"
                with open(part_file, "w", encoding="utf-8") as f:
                    for p in batch_pings:
                        rec_copy = dict(p)
                        rec_copy["timestamp_utc"] = rec_copy["timestamp_utc"].isoformat()
                        f.write(json.dumps(rec_copy) + "\n")
                written_files.append(str(part_file))

            total_pings += len(batch_pings)
            logger.debug(f"Wrote batch {part_num}: {len(batch_pings)} pings to {part_file.name}")
            del batch_pings  # Free memory immediately

        shoppers_count = sum(1 for _, p in device_assignments if p == "daily_shoppers")
        commuters_count = sum(1 for _, p in device_assignments if p == "office_commuters")
        pedestrians_count = sum(1 for _, p in device_assignments if p == "transient_pedestrians")

        summary = {
            "status": "completed",
            "total_devices": total_devices,
            "total_pings": total_pings,
            "date": date_str,
            "output_directory": str(out_path),
            "output_format": "parquet" if use_parquet else "jsonl",
            "files_written": written_files,
            "device_categories": {
                "daily_shoppers": shoppers_count,
                "office_commuters": commuters_count,
                "transient_pedestrians": pedestrians_count,
            },
        }

        logger.info(
            f"Simulation completed successfully: {total_pings:,} pings across "
            f"{total_devices} devices written to {out_path}."
        )
        return summary


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="GeoPulse Synthetic GPS Trajectory Generator CLI"
    )
    parser.add_argument(
        "--devices",
        type=int,
        default=10,
        help="Number of simulated devices (default: 10)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=1.0,
        help="Duration in hours (default: 1.0)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Target output directory (defaults to data/raw/date=YYYY-MM-DD/)",
    )

    args = parser.parse_args()

    simulator = TrajectorySimulator(seed=args.seed)
    result = simulator.run_simulation(
        device_count=args.devices,
        duration_hours=args.duration,
        output_dir=args.output_dir,
    )
    print("\n================ GeoPulse Simulation Summary ================")
    print(f"Status:            {result['status']}")
    print(f"Total Devices:     {result['total_devices']}")
    print(f"Total Pings:       {result['total_pings']:,}")
    print(f"Format:            {result['output_format']}")
    print(f"Output Directory:  {result['output_directory']}")
    print(f"Files Written:     {len(result['files_written'])}")
    print(f"Categories:        {result['device_categories']}")
    print("=============================================================\n")
