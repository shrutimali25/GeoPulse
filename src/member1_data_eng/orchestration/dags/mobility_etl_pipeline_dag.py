"""Apache Airflow DAG: GeoPulse Mobility ETL Pipeline.

Orchestrates the daily batch processing sequence:
1. Synthetic GPS generation
2. Schema & data quality validation
3. Store 500m catchment polygon generation
4. Apache Sedona / spatial join processing
5. Store visit & catchment presence detection
6. Mobility analytics computation
7. Data quality audit and pipeline reporting
"""

from datetime import datetime, timedelta

# Airflow imports with graceful fallback
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    DAG = object  # type: ignore

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="geopulse_daily_mobility_etl",
        default_args=default_args,
        description="Daily end-to-end hyper-local retail mobility pipeline",
        schedule_interval="0 2 * * *",  # 2:00 AM UTC daily
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["geopulse", "spatial", "retail_mobility"],
    ) as dag:

        def _run_gps_generation(**context):
            from src.member1_data_eng.pipeline.pipeline_orchestrator import GeoPulsePipelineOrchestrator
            orch = GeoPulsePipelineOrchestrator()
            return orch.execute_stage_1_generation().to_dict()

        def _run_preprocessing(**context):
            from src.member1_data_eng.pipeline.pipeline_orchestrator import GeoPulsePipelineOrchestrator
            orch = GeoPulsePipelineOrchestrator()
            return orch.execute_stage_2_preprocessing().to_dict()

        def _run_catchment_generation(**context):
            from src.member1_data_eng.pipeline.pipeline_orchestrator import GeoPulsePipelineOrchestrator
            orch = GeoPulsePipelineOrchestrator()
            return orch.execute_stage_3_catchments().to_dict()

        def _run_spatial_join(**context):
            from src.member1_data_eng.pipeline.pipeline_orchestrator import GeoPulsePipelineOrchestrator
            orch = GeoPulsePipelineOrchestrator()
            rec, _ = orch.execute_stage_4_spatial_join()
            return rec.to_dict()

        def _run_visit_detection(**context):
            from src.member1_data_eng.pipeline.pipeline_orchestrator import GeoPulsePipelineOrchestrator
            orch = GeoPulsePipelineOrchestrator()
            return orch.execute_stage_5_visit_detection().to_dict()

        def _run_mobility_analytics(**context):
            from src.member1_data_eng.pipeline.pipeline_orchestrator import GeoPulsePipelineOrchestrator
            orch = GeoPulsePipelineOrchestrator()
            return orch.execute_stage_6_analytics().to_dict()

        def _run_quality_audit(**context):
            from src.member1_data_eng.pipeline.pipeline_orchestrator import GeoPulsePipelineOrchestrator
            orch = GeoPulsePipelineOrchestrator()
            return orch.execute_stage_7_quality_audit().to_dict()

        task_gen = PythonOperator(
            task_id="generate_gps_telemetry",
            python_callable=_run_gps_generation,
        )
        task_prep = PythonOperator(
            task_id="preprocess_gps_data",
            python_callable=_run_preprocessing,
        )
        task_catch = PythonOperator(
            task_id="generate_store_catchments",
            python_callable=_run_catchment_generation,
        )
        task_join = PythonOperator(
            task_id="spatial_join_points_polygons",
            python_callable=_run_spatial_join,
        )
        task_visits = PythonOperator(
            task_id="detect_store_visits",
            python_callable=_run_visit_detection,
        )
        task_analytics = PythonOperator(
            task_id="compute_mobility_analytics",
            python_callable=_run_mobility_analytics,
        )
        task_audit = PythonOperator(
            task_id="data_quality_audit",
            python_callable=_run_quality_audit,
        )

        task_gen >> task_prep
        [task_prep, task_catch] >> task_join >> task_visits >> task_analytics >> task_audit
