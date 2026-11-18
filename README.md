# GeoPulse â€“ Hyper-Local Retail Mobility Analytics

[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-orange)](https://spark.apache.org/)
[![Apache Sedona](https://img.shields.io/badge/Apache%20Sedona-1.6.0-red)](https://sedona.apache.org/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Cloud-blue)](https://www.snowflake.com/)
[![dbt](https://img.shields.io/badge/dbt-1.7%2B-FF694B)](https://www.getdbt.com/)
[![Kepler.gl](https://img.shields.io/badge/Kepler.gl-React-brightgreen)](https://kepler.gl/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8%2B-teal)](https://airflow.apache.org/)

---

## 1. Project Overview

**GeoPulse** is an enterprise-grade, hyper-local retail mobility analytics platform developed as a collaborative college engineering project. The platform models consumer foot traffic, dwell durations, movement patterns, and retail catchment dynamics around commercial retail centers.

Modern physical retail requires granular, data-driven insights into customer mobility:
- *Which retail stores experience the highest foot traffic at specific hours of the day?*
- *What is the average dwell time of customers inside a mall vs. an adjacent coffee shop?*
- *What are the cross-shopping and cannibalization patterns between neighboring retail outlets?*

GeoPulse addresses these questions through an end-to-end modern data stack: generating high-resolution synthetic spatio-temporal GPS telemetry, executing distributed spatial joins with **Apache Sedona** on **PySpark**, loading structured mobility data into **Snowflake**, executing analytical modeling with **dbt**, and delivering an interactive spatio-temporal dashboard built with **React** and **Kepler.gl**, all orchestrated via **Apache Airflow**.

---

## 2. Key Objectives

1. **High-Fidelity Telemetry Simulation**: Generate synthetic GPS mobility pings modeling physical constraints, Gaussian noise, GPS multipath drift, and store attraction zones.
2. **Robust Schema Enforcement**: Enforce strict data contracts and validation rules (coordinate bounding, timestamps, speed sanity checks) using Pydantic.
3. **Distributed Spatial Data Engineering**: Perform high-throughput point-in-polygon spatial joins (`ST_Contains`) and dwell time window calculations across millions of pings with PySpark and Apache Sedona.
4. **Cloud Data Warehousing**: Ingest structured spatial metrics into Snowflake (`RAW_STAGE`) and transform them into dimensional star-schema marts using **dbt**.
5. **Interactive Mobility Visualization**: Provide business stakeholders with an interactive **Kepler.gl** and **React** dashboard displaying foot-traffic heatmaps, trajectory animations, and store performance KPIs.
6. **Automated Pipeline Orchestration**: Coordinate the daily batch pipeline seamlessly through **Apache Airflow**.

---

## 3. Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Data Generation & Validation** | Python 3.12, NumPy, Shapely, Pydantic 2 | GPS telemetry synthesis, coordinate noise, schema validation |
| **Distributed Spatial Engine** | Apache Spark 3.5.1, Apache Sedona 1.6.0 | R-tree spatial indexing, `ST_Contains` joins, dwell window functions |
| **Spatial Indexing** | Uber H3 | Hexagonal spatial binning (resolution 8 & 9) for density analytics |
| **Data Warehousing** | Snowflake Data Cloud | Cloud data storage, staging, and analytics querying |
| **Data Transformation** | dbt (Data Build Tool) Core | Staging, intermediate, and marts modeling with automated testing |
| **Pipeline Orchestration** | Apache Airflow | Directed Acyclic Graphs (DAGs) managing batch data pipelines |
| **API Serving Layer** | FastAPI, Uvicorn | High-performance asynchronous REST endpoints for GeoJSON & metrics |
| **Frontend Dashboard** | React 18, Kepler.gl, Vite, Deck.gl | Web-based spatio-temporal maps, trajectory playback, KPI cards |
| **Testing & Quality Assurance** | Pytest, Ruff, Black, Mypy | Unit testing, linting, formatting, and static typing |

---

## 4. Architecture Overview

```
                      +-----------------------------------+
                      |     Study Zone & POI Catalog      |
                      |   (GeoJSON Mall/Store Polygons)   |
                      +-----------------+-----------------+
                                        |
                                        v
+-----------------------+     +-------------------+     +-------------------------+
| Synthetic GPS Engine  | --> | Schema Validation | --> | Local Parquet Staging   |
| (Random Walk & Drift) |     |  (Pydantic v2)    |     | (data/raw/partitioned)  |
+-----------------------+     +-------------------+     +------------+------------+
                                                                     |
                                                                     v
                                                        +-------------------------+
                                                        |   PySpark + Sedona      |
                                                        |  Spatial Joins & Dwell  |
                                                        +------------+------------+
                                                                     |
                                                                     v
                                                        +-------------------------+
                                                        |   Snowflake RAW_STAGE   |
                                                        +------------+------------+
                                                                     |
                                                                     v
                                                        +-------------------------+
                                                        |  dbt Transformation     |
                                                        | (Stg -> Int -> Marts)   |
                                                        +------------+------------+
                                                                     |
                                        +----------------------------+
                                        |
                                        v
                        +-------------------------------+
                        |       FastAPI Gateway         |
                        +---------------+---------------+
                                        |
                                        v
                        +-------------------------------+
                        |  React + Kepler.gl Dashboard  |
                        |   (Interactive Visual Map)    |
                        +-------------------------------+
```

For full architectural details, see [docs/architecture.md](docs/architecture.md).

---

## 5. Engineering Roles & Module Organization

The project is structured with clear separation of engineering domains:

```
src/
â”œâ”€â”€ member1_data_eng/          <-- Data Engineering & Spatial Pipeline
â”‚   â”œâ”€â”€ generator/             <-- Synthetic GPS trajectory simulation
â”‚   â”œâ”€â”€ validation/            <-- Pydantic schema validation & quality gates
â”‚   â”œâ”€â”€ spatial_processing/    <-- PySpark & Apache Sedona spatial jobs
â”‚   â”œâ”€â”€ warehouse/             <-- Snowflake staging and loader
â”‚   â””â”€â”€ orchestration/         <-- Apache Airflow DAGs
â”œâ”€â”€ member2_analytics_viz/     <-- Analytics Engineering & Visualization
â”‚   â”œâ”€â”€ dbt_transforms/        <-- dbt models (staging, intermediate, marts)
â”‚   â”œâ”€â”€ api/                   <-- FastAPI backend serving metrics & GeoJSON
â”‚   â””â”€â”€ dashboard/             <-- React + Kepler.gl frontend application
â””â”€â”€ common/                    <-- Shared configurations, logger, and utilities
```

### Data Engineering & Spatial Analysis
- Spatial modeling, GPS trajectory simulator with noise & dwell mechanics.
- Schema validation, data contracts, and quality assertions.
- PySpark & Apache Sedona point-in-polygon spatial joins and dwell duration calculation.
- Snowflake staging and loading pipelines.
- Apache Airflow orchestration DAGs and pipeline automation.

### Analytics Engineering, BI & Frontend
- Snowflake dimensional modeling and warehouse schema definitions.
- dbt transformations (`staging`, `intermediate`, `marts`) and data integrity tests.
- FastAPI endpoints serving mobility metrics, store aggregations, and GeoJSON boundaries.
- Interactive Web Dashboard using React, Kepler.gl, Deck.gl, and Tailwind CSS.
- Metric cards (footfall volume, peak visiting hours, customer retention, cannibalization).

---

## 6. Directory Structure

```
GeoPulse/
â”œâ”€â”€ .github/
â”‚   â””â”€â”€ workflows/
â”‚       â””â”€â”€ ci.yml                     # Continuous integration workflow
â”œâ”€â”€ config/
â”‚   â”œâ”€â”€ base.yaml                      # Core project & study zone configuration
â”‚   â””â”€â”€ snowflake.template.env         # Snowflake credentials template
â”œâ”€â”€ data/
â”‚   â”œâ”€â”€ raw/                           # Raw simulated GPS pings (gitignored)
â”‚   â”œâ”€â”€ processed/                     # Cleaned spatial parquet files (gitignored)
â”‚   â”œâ”€â”€ spatial/                       # Store polygons & study zone GeoJSONs
â”‚   â”‚   â””â”€â”€ mall_boundaries.geojson   # Sample Koramangala retail POIs
â”‚   â””â”€â”€ sample/                        # Schema fixtures for unit tests
â”‚       â””â”€â”€ sample_gps_schema.json
â”œâ”€â”€ docs/
â”‚   â”œâ”€â”€ architecture.md                # System architecture specification
â”‚   â”œâ”€â”€ daily_progress.md              # Daily progress tracker & work logs
â”‚   â”œâ”€â”€ project_plan.md                # Sprint-by-sprint implementation plan
â”‚   â””â”€â”€ setup_guide.md                 # Detailed local setup & troubleshooting
â”œâ”€â”€ src/
â”‚   â”œâ”€â”€ common/                        # Shared logger & configuration loaders
â”‚   â”œâ”€â”€ member1_data_eng/              # Data engineering & spatial pipeline
â”‚   â””â”€â”€ member2_analytics_viz/         # Analytics engineering, dbt models, API & Kepler.gl dashboard
â”œâ”€â”€ tests/                             # Unit and integration test suites
â”œâ”€â”€ .env.example                       # Environment variables template
â”œâ”€â”€ .gitignore                         # Git exclusion rules
â”œâ”€â”€ pyproject.toml                     # Package & linter configuration
â”œâ”€â”€ requirements.txt                   # Lightweight core dependencies (Day 1)
â”œâ”€â”€ requirements-dev.txt               # Testing & linting tools
â”œâ”€â”€ requirements-spatial.txt           # PySpark, Sedona, GeoPandas (Phase 2)
â””â”€â”€ requirements-snowflake.txt         # Snowflake connector & dbt (Phase 3)
```

---

## 7. License
Academic Engineering Project â€“ Designed and developed for demonstration and educational purposes.
