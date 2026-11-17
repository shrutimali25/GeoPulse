# GeoPulse Daily Work Progress Tracker

## Day 1: System Inspection, Architecture Design & Project Scaffolding
**Date**: November 17, 2026  
**Engineering Domain**: Data Engineering & Platform Architecture  
**Status**: Completed

### 1. Workspace & Environment Audit Findings
- **Operating System**: Windows 64-bit (PowerShell terminal).
- **Git**: Installed (`git version 2.55.0.windows.5`). Clean workspace, uninitialized repository.
- **Python**: `Python 3.12.10` located at `C:\Users\HP\AppData\Local\Programs\Python\Python312\python.exe`.
  - *Observation*: Python 3.12 is installed, but `C:\Users\HP\AppData\Local\Programs\Python\Python312` and its `Scripts` folder are not present in the active user `PATH` (only the Python Launcher `py.exe` was in PATH).
  - *Remediation*: Virtual environment `.venv` should be created directly pointing to this binary, or user PATH updated.
- **Java Runtime**: `OpenJDK 21.0.12.1 LTS` (Temurin build 21.0.12.1+1-LTS) installed.
  - *Observation*: PySpark 3.5 requires specific JVM configuration flags on Java 21, or Eclipse Temurin JDK 11 / 17 recommended for native Apache Spark / Sedona stability on Windows.
- **Node.js & npm**: Node `v24.20.0` and npm `11.19.0` installed.
  - *Observation*: PowerShell execution policy (`PSSecurityException`) blocked `npm.ps1`. Running via `npm.cmd` or `cmd.exe /c npm` functions smoothly.
- **Docker**: Not currently present in PATH.
  - *Strategy*: Airflow and local services can run via local standalone Python mode or WSL2 / Docker Desktop once installed.

### 2. Architecture & Design Decisions
1. **Repository Layout**:
   - Monorepo design with strict domain separation:
     - `src/member1_data_eng/`: Ingestion, trajectory generation, PySpark/Sedona spatial transforms, Snowflake loading, Airflow DAGs.
     - `src/member2_analytics_viz/`: dbt transformation models, FastAPI metrics service, React + Kepler.gl dashboard.
     - `src/common/`: Shared logging, configuration loaders, and schemas.
   - Prevents code merge conflicts while keeping the data contract cohesive.
2. **Modular Dependency Management**:
   - Rather than dumping heavy Big Data dependencies on Day 1, dependencies are staged:
     - `requirements.txt`: Core lightweight utilities (Pydantic, PyYAML, loguru).
     - `requirements-dev.txt`: Quality assurance (pytest, ruff, black).
     - `requirements-spatial.txt`: Geospatial & distributed computing (Sedona, PySpark, GeoPandas).
     - `requirements-snowflake.txt`: Warehouse connector and dbt.

### 3. Deliverables Completed on Day 1
- [x] Inspected workspace and audited runtime toolchains.
- [x] Designed and created complete directory structure across all layers.
- [x] Created `README.md` with complete technical specification and system architecture.
- [x] Configured robust `.gitignore` covering Python, Spark, dbt, Node, and spatial cache artifacts.
- [x] Created environment variable template (`.env.example`) and Snowflake template (`config/snowflake.template.env`).
- [x] Authored project configuration (`config/base.yaml`) defining study zones, POIs, and simulation parameters.
- [x] Created schema definitions:
  - GeoJSON POI schema: `data/spatial/mall_boundaries.geojson`
  - Telemetry ping JSON schema: `data/sample/sample_gps_schema.json`
- [x] Authored roadmap (`docs/project_plan.md`), setup manual (`docs/setup_guide.md`), and progress tracker (`docs/daily_progress.md`).
- [x] Created clean module skeletons for both engineering domains with type hints and docstrings.
- [x] Added unit test harnesses in `tests/`.

### 4. Blockers & Risks Identified
- PySpark with Java 21 on Windows needs `HADOOP_HOME` (winutils.exe and hadoop.dll). A documented setup guide has been added to `docs/setup_guide.md`.
- No actual fake data was generated; all schemas and code skeletons are staged cleanly for Day 2 implementation.

---

## Day 2 (Phase 1): Synthetic GPS Trajectory Generation Pipeline
**Date**: November 17, 2026  
**Engineering Domain**: Data Engineering & Spatial Pipeline  
**Status**: Completed & Verified

### 1. Deliverables Completed
- [x] Implemented stochastic mobility trajectory simulator in `src/member1_data_eng/generator/trajectory_simulator.py`.
- [x] Supported three behavioral profiles (`daily_shoppers`, `office_commuters`, `transient_pedestrians`) based on configured ratios.
- [x] Implemented dwell state mechanics around retail POI polygons with low-velocity indoor jitter.
- [x] Implemented boundary reflection cushions to guarantee 100% of points remain within Koramangala study zone.
- [x] Injected Gaussian GPS observation noise ($\sigma = 4.5\text{m}$) and 5% multipath distortion.
- [x] Added optional `altitude_meters` and `is_synthetic` to `GPSPingRecord` in `src/member1_data_eng/validation/schema_validator.py`.
- [x] Updated `DataQualityValidator` to dynamically resolve bounding box coordinates from `config/base.yaml`.
- [x] Created 9 comprehensive unit tests in `tests/test_generator.py` covering counts, monotonicity, IDs, bounds, speed sanity, and schema compliance.
- [x] All 16 unit tests passing cleanly.

### 2. Next Steps
- [ ] Add remaining retail POI boundaries to `data/spatial/mall_boundaries.geojson`.
- [ ] Install Phase 2 dependencies (`pyarrow`, `pyspark`, `apache-sedona`) for distributed spatial processing.
- [ ] Implement PySpark and Apache Sedona point-in-polygon spatial join pipeline (`ST_Contains`).
