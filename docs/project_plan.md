# GeoPulse Project Plan & Implementation Roadmap

## 1. Project Overview & Scope
**GeoPulse** is an end-to-end hyper-local retail mobility analytics platform. The platform models consumer foot traffic, dwell times, and spatial catchment dynamics around retail centers by ingesting high-frequency spatio-temporal telemetry, processing it through distributed spatial engines, transforming it in Snowflake via dbt, and serving interactive spatial visualizations via Kepler.gl and React.

---

## 2. Engineering Domain Responsibilities

| Responsibility Area | Data Engineering & Spatial Pipeline | Analytics Engineering & Frontend BI |
|---|---|---|
| **Synthetic Data Generation** | Realistic GPS trajectory simulator, noise models, POI boundaries | Validates data requirements & schema alignment |
| **Data Quality & Validation** | Schema validation (Pydantic), coordinate bounding, deduplication | Data contract definitions & tests |
| **Distributed Spatial Engine**| PySpark & Apache Sedona spatial joins, dwell detection, H3 indexing | Consumes processed parquet tables |
| **Data Warehouse Ingestion**  | Snowflake raw ingestion (Snowpark / COPY INTO / staged parquet) | Snowflake RBAC & warehouse schema modeling |
| **Data Transformation (dbt)** | Reviews data warehouse schema contracts | dbt models (staging, intermediate, marts), dbt tests |
| **API & Serving Layer**       | Ingestion APIs / raw telemetry streaming interfaces | FastAPI backend serving analytical aggregations |
| **Frontend & Visualization**  | Supplies spatial GeoJSONs & Kepler configuration specs | React + Kepler.gl web dashboard, charts, UI |
| **Orchestration & DevOps**    | Apache Airflow DAGs, CI pipeline, testing harnesses | Airflow dbt operator integration, frontend build CI |

---

## 3. Sprint-by-Sprint Implementation Roadmap

### Sprint 1: Foundation & Data Synthesis (Days 1 - 3)
- **Day 1: Repository Foundation & Compatibility Audit (Completed)**
  - Establish clean multi-module repository structure.
  - Audit Python 3.12, OpenJDK 21, Node.js 24, Git environments.
  - Define data schemas, base configs, and modular dependencies.
- **Day 2: GPS Trajectory Generation Engine (Data Engineering)**
  - Implement mathematical mobility model: random-walk drift, POI attractors, dwell states.
  - Implement Pydantic schema validation for GPS ping records.
  - Set up dbt project repository and Snowflake trial environment.
- **Day 3: Baseline Synthetic Dataset & Local Validation (Integrated Pipeline)**
  - Generate baseline test dataset (500 devices, 24 hours, ~50,000 pings).
  - Verify speed, acceleration, and bounding box compliance without data corruption.
  - Scaffold React + Vite application with Kepler.gl dependencies.

### Sprint 2: Distributed Spatial Processing & Warehousing (Days 4 - 7)
- **Day 4: Apache Sedona & PySpark Spatial Join Pipeline (Spatial Data Engineering)**
  - Configure Spark session with Sedona spatial extensions.
  - Implement `ST_Contains` spatial join between GPS points and retail POI polygons.
  - Compute dwell times using window functions (timestamp delta per device per store).
- **Day 5: Spatial Aggregations & H3 Spatial Indexing (Spatial Data Engineering)**
  - Add Uber H3 hexagonal spatial indexing for regional foot-traffic density.
  - Save partitioned Parquet outputs (`date=YYYY-MM-DD/store_id=...`).
- **Day 6: Snowflake Staging & Ingestion (Data Engineering & Warehouse)**
  - Build automated Snowflake loader (Snowpark / stage upload).
  - Define Snowflake staging schemas (`RAW_STAGE`, `ANALYTICS`).
- **Day 7: dbt Data Modeling & Metrics Marts (Analytics Engineering)**
  - Build `stg_gps_pings`, `int_store_dwell_times`, and `fct_hourly_footfall`.
  - Add dbt generic tests (uniqueness, not-null, referential integrity).

### Sprint 3: Orchestration, API & Interactive Dashboard (Days 8 - 11)
- **Day 8: Apache Airflow DAG Pipeline (Data Engineering & Orchestration)**
  - Create end-to-end Airflow DAG: Generate -> Validate -> Spark Sedona -> Snowflake -> dbt run.
  - Configure task dependencies, retries, and SLA alerts.
- **Day 9: Serving API & Kepler.gl Integration (Backend & Frontend)**
  - Build FastAPI endpoints for store metrics, catchment polygons, and mobility heatmaps.
  - Wire Kepler.gl state into React Redux store.
- **Day 10: Dashboard Analytics & Visual Polish (Frontend Dashboard)**
  - Build UI metric cards (peak foot traffic hour, average dwell duration, store churn).
  - Add time-playback animation of simulated pedestrian trajectories.
- **Day 11: End-to-End Integration Testing & Benchmarking (Full Platform)**
  - Run full pipeline with 1,000,000 telemetry points.
  - Benchmark Spark processing throughput and Snowflake query latency.

### Sprint 4: Final Polishing, Documentation & Presentation (Days 12 - 14)
- **Day 12: Comprehensive Documentation & Architecture Diagrams**
  - Finalize API docs, setup guides, and reproducible execution scripts.
- **Day 13: Edge Case Handling & Unit Test Coverage**
  - Add comprehensive unit tests (target > 80% test coverage).
  - Verify error handling on corrupted coordinates and missing timestamps.
- **Day 14: Project Demo & Final Report**
  - Record project demonstration and prepare final presentation slides.

---

## 4. Key Milestones & Success Criteria
1. **Milestone 1 (Day 3)**: Fully functional GPS generator producing valid, schema-compliant spatio-temporal trajectories.
2. **Milestone 2 (Day 5)**: Apache Sedona pipeline successfully calculating store visits and dwell times from raw coordinates.
3. **Milestone 3 (Day 7)**: Snowflake data warehouse populated and transformed with tested dbt models.
4. **Milestone 4 (Day 10)**: Interactive Kepler.gl dashboard visualizing spatial trajectories and retail analytics in real-time.
5. **Milestone 5 (Day 12)**: Automated Airflow DAG orchestrating the end-to-end pipeline reliably.
