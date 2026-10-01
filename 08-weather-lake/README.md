# 08 — Weather Data Lake (S3 -> PostgreSQL -> dbt -> Airflow)

## Goal

Build a small end-to-end data pipeline: object storage -> Python ingestion -> PostgreSQL -> dbt transformation/tests -> Airflow orchestration, using a real-world dataset instead of a synthetic CSV. The project was built fully independently from earlier projects, with its own `.venv`, RustFS bucket, `.env`, and a separate, dedicated Airflow + Docker setup.

## Dataset

Seattle daily weather data, 2012-2015 (1461 rows), downloaded from Kaggle.

Columns: `date`, `precipitation`, `temp_max`, `temp_min`, `wind`, `weather`.

## Pipeline

```text
seattle-weather.csv (local)
    |
    | aws s3 cp
    v
RustFS bucket: weather-lake
    -> raw/seattle-weather.csv
    |
    v
Airflow DAG: weather_pipeline
    |
    +-- ingest (PythonOperator)
    |     - reads raw/seattle-weather.csv from RustFS via boto3
    |     - loads it into PostgreSQL (weather_data) via pandas.to_sql,
    |       using TRUNCATE + append inside one transaction
    |       (preserves the table and any dependent dbt views,
    |       unlike to_sql's own "replace", which drops the table)
    |
    +-- dbt_run (BashOperator)
    |     stg_weather  (type casting, column cleanup)
    |     weather_monthly (ref()'d from stg_weather: monthly aggregates)
    |
    +-- dbt_test (BashOperator)
    |     unique / not_null / accepted_values checks on stg_weather
    |
    v
ingest -> dbt_run -> dbt_test   (SUCCESS)
```

## Stack

- Python, boto3, pandas, SQLAlchemy, psycopg2
- PostgreSQL
- RustFS (S3-compatible object storage)
- AWS CLI
- dbt Core 1.12.5 + dbt-postgres
- Apache Airflow 3.3.2, Docker Compose, with a custom image (`FROM apache/airflow:3.3.2` + `dbt-postgres`)

## Project Structure

```text
08-weather-lake/
├── docker-compose.yaml        (official Airflow quickstart, adapted)
├── Dockerfile                 (adds dbt-postgres to the Airflow image)
├── .env                        (AIRFLOW_UID, FERNET_KEY, POSTGRES_*, AWS_*  - gitignored)
├── airflow/
│   ├── dags/weather_pipeline.py
│   ├── logs/
│   └── plugins/
├── dbt/weather_dbt/
│   ├── models/staging/stg_weather.sql
│   ├── models/weather_monthly.sql
│   ├── models/schema.yml
│   └── .container/profiles.yml   (container-only profile, host.docker.internal)
├── src/read_from_s3.py
├── sql/weather_analysis.sql
└── README.md
```

## What I Practiced

- Set up a project fully independent from earlier ones: own `.venv`, own RustFS bucket, own Airflow/Docker stack (not reusing a prior day's Airflow setup)
- Wrote the S3 bucket/upload commands independently, reusing what was learned on a prior cloud session
- Read an S3 object with boto3, converted it to a pandas DataFrame (`BytesIO`), loaded it into PostgreSQL
- Built a two-model dbt project (`stg_weather` -> `weather_monthly` via `ref()`) with `unique`, `not_null`, and `accepted_values` tests
- Wrote SQL window-function analytics directly against the dataset (monthly/yearly aggregates with `date_trunc`/`extract`, `LAG()` for day-over-day change, a 7-day moving average with `rows between 6 preceding and current row`)
- Built a second, independent Airflow + Docker environment from the official quickstart Compose file, with its own Dockerfile extending the Airflow image with dbt
- Debugged a full container-to-host networking chain from scratch: DNS (`host.docker.internal` via `extra_hosts: host-gateway`), firewall (UFW rule for the new Docker subnet), and PostgreSQL's `pg_hba.conf` (new subnet entry) - the same category of problem as a prior project, this time diagnosed independently, layer by layer
- Connected a second container (RustFS) into the new project's Docker network so the Airflow worker could reach it by service name instead of an IP or host address
- Fixed a `ModuleNotFoundError` caused by the DAG file and the ingestion script living in separate mounted directories (`/opt/airflow/dags` vs `/opt/airflow/src`), via `sys.path.append`
- Hit and fixed a real `pandas.to_sql(if_exists="replace")` failure: PostgreSQL refused to drop `weather_data` because dbt-created views depended on it; fixed with `TRUNCATE` + `append` inside one transaction instead of drop-and-recreate
- Ran the full DAG (`ingest -> dbt_run -> dbt_test`) to a successful end-to-end result, confirmed in both the CLI and the Airflow UI

## Scope

Incremental loading / upsert logic was intentionally left out here (the table is fully reloaded each run via TRUNCATE + append) since that pattern was already built and tested in an earlier project. The focus of this project was the S3 -> PostgreSQL -> dbt -> Airflow integration itself, and the container networking required to make it work as an independent stack.