# Mini ETL Pipeline — Python + PostgreSQL + dbt + Airflow

## Project Overview

This project is part of my Data Engineering learning journey.

It started as a small standalone pipeline:

CSV -> Python -> PostgreSQL -> dbt

and was later integrated into Apache Airflow as an orchestrated pipeline running inside Docker.

## Pipeline

```text
orders.csv
    |
    v
Airflow DAG: mini_etl_pipeline
    |
    +-- ingest
    |     - reads watermark
    |     - filters new rows
    |     - performs idempotent upserts
    |     - updates watermark after successful commit
    |
    v
mini_orders (PostgreSQL)
    |
    v
dbt run
    |
    v
customer_revenue
    |
    v
dbt test
```

The Airflow tasks run sequentially:

```
ingest -> dbt_run -> dbt_test
```

## Technologies

- Python
- PostgreSQL
- dbt Core + dbt PostgreSQL adapter
- Apache Airflow 3.3.1
- Docker Compose

## Key Design Points

### Idempotency

The ingestion uses:

```sql
INSERT ... ON CONFLICT (order_id) DO UPDATE
```

Re-running the same input does not create duplicate rows.

### Incremental Loading

A watermark stored in `mini_pipeline_state` is used to process only rows newer than the previously processed date.

### Transaction Safety

The ingestion runs inside a PostgreSQL transaction. The watermark is updated only after the data load succeeds and the transaction is committed.

A deliberate failure was tested during the pipeline. The transaction rolled back successfully, leaving both the data and watermark unchanged.

### Airflow Orchestration

Airflow controls the execution order:

```
ingest -> dbt run -> dbt test
```

This separates ingestion from transformation and validation.

### Docker + dbt

The standard Airflow image was extended with a small Dockerfile:

```dockerfile
FROM apache/airflow:3.3.1

RUN pip install --no-cache-dir dbt-postgres
```

This provides an Airflow environment where dbt is available to the pipeline tasks.

### Container -> Host PostgreSQL

Because PostgreSQL runs on the host machine while Airflow runs inside Docker, the pipeline uses:

```
host.docker.internal
```

instead of:

```
localhost
```

### Secrets

The PostgreSQL password is provided through environment variables and is not hardcoded in the Python or dbt configuration.

The local `profiles.yml` containing the connection configuration is excluded from Git.

## How to Run

From the Airflow project directory:

```bash
cd 04-airflow/day09

docker compose up -d --build

docker compose exec airflow-worker airflow dags unpause mini_etl_pipeline

docker compose exec airflow-worker airflow dags trigger mini_etl_pipeline

docker compose exec airflow-worker airflow dags list-runs mini_etl_pipeline
```

## What I Learned

- A pipeline can behave differently inside Docker compared with running directly on the host.
- Container networking, file paths and environment variables are important parts of a data pipeline.
- PostgreSQL transactions provide rollback when a pipeline fails before commit.
- Incremental processing and idempotency solve different problems and are both important for reliable pipelines.
- Airflow can orchestrate ingestion, transformation and testing as dependent tasks.
- dbt separates transformation logic from pipeline orchestration.
- Getting a pipeline to run successfully and being able to debug it confidently are different skills.

Debugging Airflow, Docker and networking issues is an area I will continue to develop through further hands-on projects.

## Linux Debugging Basics

While debugging this project's setup (missing `.venv`, wrong host address, missing `.env`), a small set of Linux commands turned out to be the standard way to isolate where a problem actually is — checking one layer at a time rather than guessing.

### Process
```bash
ps aux | grep <process>
kill <PID>
```
Find a running process and its PID; stop it if needed.

### Resources
```bash
top
```
Live view of CPU/memory usage per process.

### Services
```bash
systemctl --type=service --state=running
sudo systemctl stop <service>
```
List running background services (e.g. database servers); stop one cleanly instead of killing its process directly.

### Disk
```bash
df -h
du -sh <directory>
du -sh <directory>/*
```
`df` shows overall filesystem usage; `du` shows which directory is actually using the space.

### Network
```bash
ss -ltn
nc -zv localhost <port>
```
`ss` checks whether a service is listening on a port; `nc` checks whether that port is actually reachable.

### Database connectivity
```bash
psql -h localhost -p 5432 -U <user> -d <database>
```
Confirms the connection works at the authentication/database level, not just the network level.

### The debugging layers, in order
```
Is the process running?      -> ps
Is it using CPU/memory?      -> top
Is the service up?           -> systemctl
Is there disk space?         -> df / du
Is the port listening?       -> ss
Is the port reachable?       -> nc
Can I actually connect?      -> psql
```
