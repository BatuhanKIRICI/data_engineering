from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

from mini_ingest import ingest

with DAG(
    dag_id="mini_etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest,
    )

    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dags/dbt && dbt run --profiles-dir /opt/airflow/dags/dbt",
    )

    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dags/dbt && dbt test --profiles-dir /opt/airflow/dags/dbt",
    )

    ingest_task >> dbt_run_task >> dbt_test_task
