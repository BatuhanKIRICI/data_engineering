FROM apache/airflow:3.3.2

ARG AIRFLOW_VERSION=3.3.2

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "dbt-postgres==1.11.0"