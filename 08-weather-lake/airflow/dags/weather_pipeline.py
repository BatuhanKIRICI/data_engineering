import sys
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

sys.path.append("/opt/airflow")

from src.read_from_s3 import ingest

with DAG(
    dag_id="weather_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "dbt run "
            "--project-dir /opt/airflow/dbt/weather_dbt "
            "--profiles-dir /opt/airflow/dbt/weather_dbt/.container"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "dbt test "
            "--project-dir /opt/airflow/dbt/weather_dbt "
            "--profiles-dir /opt/airflow/dbt/weather_dbt/.container"
        ),
    )

    ingest_task >> dbt_run >> dbt_test
