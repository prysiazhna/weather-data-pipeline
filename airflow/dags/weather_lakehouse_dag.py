from __future__ import annotations

from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from src.ingestion.write_bronze import run as write_bronze_run
from src.transforms.bronze_to_silver_daily import run as bronze_to_silver_run
from src.quality.silver_checks_daily import run as quality_gate_run
from src.ingestion.loaders.postgres_loader_daily import run as load_postgres_daily_run
from src.ingestion.loaders.postgres_loader_locations import run as load_postgres_locations_run


DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt/weather_dbt"

default_args = {
    "owner": "data",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="weather_lakehouse_daily",
    description="End-to-end: WeatherAPI -> Bronze -> Silver -> Quality -> Postgres -> dbt",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 12, 28, tz="UTC"),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["weather", "lakehouse", "bronze", "silver", "dbt"],
) as dag:

    
    dt_arg = {"dt": "{{ ds }}"}

    with TaskGroup(group_id="tg_bronze", tooltip="Extract WeatherAPI -> MinIO bronze") as tg_bronze:
        write_bronze = PythonOperator(
            task_id="write_bronze",
            python_callable=write_bronze_run,
            op_kwargs=dt_arg,
            execution_timeout=timedelta(minutes=15),
        )

    with TaskGroup(group_id="tg_silver", tooltip="Bronze -> Silver parquet") as tg_silver:
        bronze_to_silver = PythonOperator(
            task_id="bronze_to_silver",
            python_callable=bronze_to_silver_run,
            op_kwargs=dt_arg,
            execution_timeout=timedelta(minutes=20),
        )

    with TaskGroup(group_id="tg_quality", tooltip="Quality gate on Silver") as tg_quality:
        quality_gate = PythonOperator(
            task_id="quality_gate",
            python_callable=quality_gate_run,
            op_kwargs=dt_arg,
            execution_timeout=timedelta(minutes=10),
        )

    with TaskGroup(group_id="tg_load", tooltip="Load Silver -> Postgres staging") as tg_load:
        load_locations_staging = PythonOperator(
            task_id="load_locations_staging",
            python_callable=load_postgres_locations_run,
            op_kwargs=dt_arg,
            execution_timeout=timedelta(minutes=10),
        )

        load_weather_daily_staging = PythonOperator(
            task_id="load_weather_daily_staging",
            python_callable=load_postgres_daily_run,
            op_kwargs=dt_arg,
            execution_timeout=timedelta(minutes=15),
        )

        load_locations_staging >> load_weather_daily_staging

    with TaskGroup(group_id="tg_dbt", tooltip="dbt deps/run/test/freshness") as tg_dbt:
        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}",
            execution_timeout=timedelta(minutes=5),
        )

        dbt_run_core = BashOperator(
            task_id="dbt_run_core",
            bash_command=(
                f"cd {DBT_PROJECT_DIR} && "
                f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select +path:models/marts"
            ),
            execution_timeout=timedelta(minutes=15),
        )

        dbt_run_bi = BashOperator(
            task_id="dbt_run_bi",
            bash_command=(
                f"cd {DBT_PROJECT_DIR} && "
                f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select +path:models/bi"
            ),
            execution_timeout=timedelta(minutes=15),
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}",
            execution_timeout=timedelta(minutes=15),
        )

        dbt_source_freshness = BashOperator(
            task_id="dbt_source_freshness",
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt source freshness --profiles-dir {DBT_PROFILES_DIR}",
            execution_timeout=timedelta(minutes=10),
        )

        dbt_deps >> dbt_run_core >> dbt_run_bi >> dbt_test >> dbt_source_freshness

    tg_bronze >> tg_silver >> tg_quality >> tg_load >> tg_dbt