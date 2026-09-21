# Mini ETL Pipeline — Python + PostgreSQL + dbt + Airflow — Consolidation Project

## Project Overview

This project is part of my Data Engineering learning journey. After several concept-heavy days (Day 8-11: dbt, Airflow, idempotency, incremental processing, watermarks), this project builds one small pipeline that actually uses all of them together, end to end — built and debugged hands-on rather than discussed in theory.

## Pipeline

```
orders.csv
    |
    v
Python ingestion (mini_ingest.py)
    |
    +-- reads watermark from mini_pipeline_state
    +-- filters rows: only created_at > watermark
    +-- INSERT ... ON CONFLICT (order_id) DO UPDATE  (idempotent)
    |
    v
mini_orders (PostgreSQL)
    |
    v
dbt run
    |
    v
customer_revenue (per-customer totals)
    |
    v
dbt test (not_null checks)
    |
    v
Airflow (currently orchestrates the ingest step only; dbt run/test were executed manually, not yet wired into the DAG as separate tasks)
```

## Technologies

- Python (`psycopg2`, `python-dotenv`)
- PostgreSQL
- dbt-core + dbt-postgres
- Apache Airflow (Docker Compose, reusing the Day 9 Airflow environment)

## Tests Actually Run

1. **First run**: all rows loaded, watermark set to the latest processed date.
2. **Second run, no new data**: all rows skipped, watermark unchanged — incremental filtering confirmed.
3. **New row added**: only that row processed, row count increased by exactly one, no duplicates — incremental + idempotency confirmed together.
4. **dbt run**: `customer_revenue` model built from `mini_orders`, correct per-customer totals confirmed in PostgreSQL.
5. **dbt test**: `not_null` checks on `customer_revenue` passed.
6. **Airflow orchestration**: the ingestion script wrapped in an Airflow DAG (`mini_etl_pipeline`), triggered manually, and run to `success` inside the same Airflow environment set up on Day 9.

## The Debugging Journey (Airflow integration)

Getting the already-working local pipeline to run *inside* Airflow surfaced a sequence of separate, real problems — each one only visible after the previous one was fixed:

1. **DAG paused by default**: `airflow dags trigger` queued the run but nothing executed, because a newly discovered DAG starts paused in Airflow. Fixed with `airflow dags unpause`. Lesson: correct DAG code doesn't guarantee it runs — an operational setting can silently prevent execution.
2. **`localhost` inside a container is not the host machine**: the ingestion script connected to `localhost`, which inside the Airflow worker container refers to the container itself, not the host machine running PostgreSQL. Fixed by switching to `host.docker.internal` (the same fix already set up in Day 9's `docker-compose.yaml` via `extra_hosts`).
3. **`.env` not available inside the container**: the script read `POSTGRES_PASSWORD` via `python-dotenv`, but the `.env` file living in the `06-mini-etl-pipeline` project on the host was never copied or mounted into the Airflow container, so the password read as empty (`fe_sendauth: no password supplied`). Fixed by adding the password to Airflow's own `.env` (`04-airflow/day09/.env`) and passing it through to the worker service in `docker-compose.yaml`.
4. **Relative file paths behave differently in a container**: the script opened `data/orders.csv` (relative to wherever the script runs), which resolved differently once running inside the Airflow container's filesystem. Fixed by copying the CSV into `dags/data/orders.csv` and using an absolute path (`/opt/airflow/dags/data/orders.csv`).

Each of these was diagnosed by reading the actual task log rather than guessing, and fixed one layer at a time.

## Known Shortcut (documented, not hidden)

The DAG currently runs the ingestion script via:
```python
python_callable=lambda: exec(open("/opt/airflow/dags/mini_ingest.py").read())
```
This is a deliberate, temporary bridge to get Python -> Airflow working end to end without introducing packaging/deployment concepts not yet covered. It is not how a production DAG would call ingestion logic (a proper Python module/import, or a dedicated operator, would replace this) — left as-is intentionally rather than expanding scope further this session.

Similarly, `06-mini-etl-pipeline/ingest.py` (runnable directly from the host, using `localhost`) and `04-airflow/day09/dags/mini_ingest.py` (used by Airflow, using `host.docker.internal` and absolute paths) are now two intentionally different copies for two different runtime environments, rather than one shared file — a real limitation of this quick setup, not an oversight.

## What I Learned

- A pipeline is not just code — the same Python script behaves differently depending on its runtime environment (container vs. host), and getting it to actually run end to end required reasoning about networking, environment variables, and the filesystem, not just the script's logic.
- Debugging a multi-layer problem (DAG state, network addressing, credentials, file paths) one layer at a time, verifying each with a real log or command output, found the actual cause faster than guessing or changing several things at once.
- Idempotency and incremental loading, which were purely conceptual after Day 10-11, became concrete once tested with real before/after row counts and a real watermark value moving forward only on successful runs.
- Some engineering shortcuts (like the `exec()` bridge here) are fine to take deliberately, as long as they're documented as shortcuts rather than presented as the right way to do it.
