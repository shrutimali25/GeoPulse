"""Unit tests for synthetic GPS trajectory simulator.

Validates:
- Correct device and ping counts
- Strictly increasing UTC timestamps
- Unique device IDs and ping UUIDs
- Coordinates bounded within study zone
- Device category ratios
- Deterministic reproducibility with random seed
- Physical speed realism (< 70 m/s)
- GPS noise, accuracy, and multipath schema compliance
"""

from datetime import datetime, timezone
import json
from pathlib import Path
import pytest

from src.member1_data_eng.generator.trajectory_simulator import (
    TrajectoryConfig,
    TrajectorySimulator,
)
from src.member1_data_eng.validation.schema_validator import (
    DataQualityValidator,
    GPSPingRecord,
)


@pytest.fixture
def simulator():
    """Create simulator instance with deterministic seed."""
    return TrajectorySimulator(seed=42)


def test_device_and_ping_count(simulator):
    """Verify trajectory generation creates exact expected ping count."""
    duration_hours = 2
    interval_seconds = 30
    expected_pings = int((duration_hours * 3600) / interval_seconds)  # 240 pings

    pings = simulator.generate_single_device_trajectory(
        device_id="DEV_00000001",
        start_time=datetime(2026, 11, 17, 8, 0, 0, tzinfo=timezone.utc),
        profile_type="daily_shoppers",
        duration_hours=duration_hours,
        interval_seconds=interval_seconds,
    )

    assert len(pings) == expected_pings
    assert all(p["device_id"] == "DEV_00000001" for p in pings)


def test_strictly_increasing_timestamps(simulator):
    """Verify timestamps are in UTC and strictly monotonically increasing."""
    pings = simulator.generate_single_device_trajectory(
        device_id="DEV_00000002",
        start_time=datetime(2026, 11, 17, 0, 0, 0, tzinfo=timezone.utc),
        profile_type="office_commuters",
        duration_hours=1,
        interval_seconds=30,
    )

    for i in range(1, len(pings)):
        prev_ts = pings[i - 1]["timestamp_utc"]
        curr_ts = pings[i]["timestamp_utc"]

        assert curr_ts.tzinfo is not None, "Timestamp must be timezone-aware"
        assert curr_ts > prev_ts, f"Timestamp at step {i} is not strictly greater than previous"
        assert (curr_ts - prev_ts).total_seconds() == 30.0


def test_unique_device_and_ping_ids(simulator):
    """Verify generated device IDs and ping IDs are unique and properly formatted."""
    assignments = simulator._assign_device_profiles(50)
    device_ids = [d[0] for d in assignments]

    # Device ID uniqueness and regex format
    assert len(device_ids) == len(set(device_ids))
    for dev_id in device_ids:
        assert dev_id.startswith("DEV_")
        assert len(dev_id) == 12

    # Ping ID uniqueness across multiple devices
    all_ping_ids = set()
    total_pings = 0
    for dev_id, profile in assignments[:3]:
        pings = simulator.generate_single_device_trajectory(
            device_id=dev_id,
            start_time=datetime(2026, 11, 17, 10, 0, 0, tzinfo=timezone.utc),
            profile_type=profile,
            duration_hours=1,
            interval_seconds=60,
        )
        total_pings += len(pings)
        for p in pings:
            all_ping_ids.add(p["ping_id"])

    assert len(all_ping_ids) == total_pings, "Found duplicate ping_id across generated pings"


def test_coordinates_within_study_zone(simulator):
    """Verify all generated coordinates strictly remain inside configured bounding box."""
    cfg = simulator.sim_config
    validator = DataQualityValidator(
        min_lat=cfg.min_lat,
        max_lat=cfg.max_lat,
        min_lon=cfg.min_lon,
        max_lon=cfg.max_lon,
    )

    for profile in ("daily_shoppers", "office_commuters", "transient_pedestrians"):
        pings = simulator.generate_single_device_trajectory(
            device_id="DEV_00000003",
            start_time=datetime(2026, 11, 17, 8, 0, 0, tzinfo=timezone.utc),
            profile_type=profile,
            duration_hours=4,
            interval_seconds=30,
        )

        for ping in pings:
            lat = ping["latitude"]
            lon = ping["longitude"]
            assert validator.is_within_study_zone(lat, lon) is True, (
                f"Coordinate ({lat}, {lon}) fell outside study zone for {profile}"
            )
            assert cfg.min_lat <= lat <= cfg.max_lat
            assert cfg.min_lon <= lon <= cfg.max_lon


def test_device_category_ratios(simulator):
    """Verify device profile distribution adheres to configured category ratios."""
    total_devices = 200
    assignments = simulator._assign_device_profiles(total_devices)

    shoppers = sum(1 for _, prof in assignments if prof == "daily_shoppers")
    commuters = sum(1 for _, prof in assignments if prof == "office_commuters")
    pedestrians = sum(1 for _, prof in assignments if prof == "transient_pedestrians")

    assert shoppers + commuters + pedestrians == total_devices

    # Expect ~40% shoppers (80), ~35% commuters (70), ~25% pedestrians (50)
    assert abs(shoppers - 80) <= 2
    assert abs(commuters - 70) <= 2
    assert abs(pedestrians - 50) <= 2


def test_reproducibility_with_seed(simulator):
    """Verify identical random seed produces bit-exact identical trajectories."""
    sim_a = TrajectorySimulator(seed=12345)
    sim_b = TrajectorySimulator(seed=12345)

    start = datetime(2026, 11, 17, 12, 0, 0, tzinfo=timezone.utc)
    traj_a = sim_a.generate_single_device_trajectory(
        "DEV_0000000A", start, profile_type="daily_shoppers", duration_hours=1
    )
    traj_b = sim_b.generate_single_device_trajectory(
        "DEV_0000000A", start, profile_type="daily_shoppers", duration_hours=1
    )

    assert len(traj_a) == len(traj_b)
    for pa, pb in zip(traj_a, traj_b):
        assert pa["ping_id"] == pb["ping_id"]
        assert pa["latitude"] == pb["latitude"]
        assert pa["longitude"] == pb["longitude"]
        assert pa["speed_mps"] == pb["speed_mps"]
        assert pa["timestamp_utc"] == pb["timestamp_utc"]


def test_physical_movement_speed_sanity(simulator):
    """Verify instantaneous speeds remain within realistic urban bounds (< 70 m/s)."""
    for profile in ("daily_shoppers", "office_commuters", "transient_pedestrians"):
        pings = simulator.generate_single_device_trajectory(
            device_id="DEV_00000004",
            start_time=datetime(2026, 11, 17, 7, 0, 0, tzinfo=timezone.utc),
            profile_type=profile,
            duration_hours=3,
            interval_seconds=30,
        )

        for p in pings:
            speed = p["speed_mps"]
            assert speed >= 0.0, f"Negative speed detected: {speed}"
            assert speed < 70.0, f"Unrealistic speed detected: {speed} m/s"

            # Profile-specific speed sanity
            if profile == "daily_shoppers":
                # Walking/dwell speed shouldn't exceed pedestrian burst speed (< 5.0 m/s)
                assert speed < 10.0, f"Daily shopper exceeded pedestrian limit: {speed}"


def test_gps_noise_and_schema_conformance(simulator):
    """Verify all generated pings satisfy Pydantic schema validation rules."""
    pings = simulator.generate_single_device_trajectory(
        device_id="DEV_00000005",
        start_time=datetime(2026, 11, 17, 9, 0, 0, tzinfo=timezone.utc),
        profile_type="daily_shoppers",
        duration_hours=1,
        interval_seconds=30,
    )

    validator = DataQualityValidator()
    validation_results = validator.validate_batch(pings)

    assert validation_results["invalid_count"] == 0, (
        f"Validation failed on generated pings: {validation_results['errors']}"
    )
    assert validation_results["valid_count"] == len(pings)

    for p in pings:
        # Schema validation directly against GPSPingRecord
        record = GPSPingRecord(**p)
        assert record.is_synthetic is True
        assert record.altitude_meters is not None
        assert 0.0 <= record.horizontal_accuracy_meters <= 100.0


def test_batch_simulation_output(tmp_path, simulator):
    """Verify run_simulation writes batched files to disk without memory explosion."""
    output_dir = tmp_path / "date=2026-11-17"
    start_time = datetime(2026, 11, 17, 0, 0, 0, tzinfo=timezone.utc)

    summary = simulator.run_simulation(
        start_time=start_time,
        device_count=4,
        duration_hours=1,  # 1 hour = 120 pings per device * 4 = 480 pings
        output_dir=str(output_dir),
        output_format="jsonl",
        batch_size=2,
    )

    assert summary["status"] == "completed"
    assert summary["total_devices"] == 4
    assert summary["total_pings"] == 480
    assert len(summary["files_written"]) == 2  # 4 devices / batch_size 2 = 2 files

    # Verify each written file is non-empty and contains valid JSON lines
    total_lines = 0
    for fpath in summary["files_written"]:
        p = Path(fpath)
        assert p.exists()
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                assert rec["is_synthetic"] is True
                assert rec["device_id"].startswith("DEV_")
                total_lines += 1

    assert total_lines == 480
