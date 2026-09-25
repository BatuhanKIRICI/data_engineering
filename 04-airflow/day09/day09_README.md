# Airflow + Docker — First Orchestration Pipeline — Day 9

## Project Overview

This project is part of my Data Engineering learning journey. The goal of Day 9 was to move from "SQL/dbt know *how* to transform data" to "Airflow knows *when and in what order* things run" — building a first real DAG that reads a CSV, cleans it, and loads it into PostgreSQL, including retry and failure-handling behavior.

## Technologies

- Apache Airflow 3.3.1 (Docker Compose, official quick-start setup)
- Docker / Docker Compose
- PostgreSQL (host machine)
- Python (PythonOperator)
- dbt Core 1.12.5 (`dbt-postgres`)
- RustFS (S3-compatible object storage)
- Boto3

## What I Built

### Pipeline
```
orders_dirty.csv
      |
      v
   extract   (reads the raw CSV)
      |
      v
    clean    (drops malformed rows and rows missing a required field)
      |
      v
orders_clean.csv
      |
      v
    load     (PostgresHook -> orders_clean table on the host Postgres)
```

- **DAG**: `day09_pipeline`, `schedule="@daily"`, `retries=2`, `retry_delay=1 minute`
- **Dependency**: `extract_task >> clean_task >> load_task`
- 8 raw rows in -> 6 valid rows out (1 dropped for a malformed column count, 1 dropped for a missing `customer` value)
- `load` uses `PostgresHook` with an Airflow Connection (`postgres_host`) rather than a hardcoded password in the DAG code

### Failure/retry test
Deliberately raised an exception inside `extract()` to observe real failure propagation:
```
extract  -> FAILED (after exhausting its configured retries)
clean    -> upstream_failed (never ran)
load     -> upstream_failed (never ran)
```
This confirmed that Airflow's dependency graph doesn't just define order — it prevents downstream tasks from running on top of a failed upstream step, rather than silently continuing with missing or bad data.

### Mini ETL integration — Object Storage + dbt

After the initial Airflow pipeline, I extended the setup into a small end-to-end ETL pipeline using local S3-compatible object storage.

The updated flow is:

```text
RustFS
  |
  | raw/orders.csv
  v
Airflow ingest
  |
  v
PostgreSQL
  |
  v
dbt run
  |
  v
dbt test
```

The Airflow DAG is:

```
ingest
  |
  v
dbt_run
  |
  v
dbt_test
```

**Ingest**

The `ingest` task:
- reads `raw/orders.csv` from RustFS through the S3 API
- uses Boto3 to access the object
- applies the PostgreSQL watermark
- loads new records into `mini_orders`
- uses `ON CONFLICT (order_id) DO UPDATE` for idempotent writes
- updates `mini_pipeline_state` after processing

**dbt**

The dbt task transforms the loaded PostgreSQL data:

```
mini_orders
     |
     v
customer_revenue
```

The final `dbt_test` task validates the generated model.

This creates a complete separation of responsibilities:
- Airflow -> when, order, dependencies, failure handling
- Python -> ingestion logic
- PostgreSQL -> persistent storage and pipeline state
- dbt -> SQL transformations and data tests
- RustFS -> object storage

This ingestion is designed to be idempotent: re-running the same source data does not create duplicate records because the load uses `ON CONFLICT (order_id) DO UPDATE`.

## The Debugging Journey (the real lesson of the day)

Getting the Airflow container to talk to PostgreSQL on the host turned into a multi-layer networking problem, solved one layer at a time instead of changing several things at once:

1. **DNS**: `host.docker.internal` isn't automatically available on native Docker Engine (Linux) the way it is on Docker Desktop (macOS/Windows) — fixed by adding `extra_hosts: ["host.docker.internal:host-gateway"]` to the Airflow services in `docker-compose.yaml`.
2. **PostgreSQL bind address**: Postgres was only listening on `localhost` (`listen_addresses = 'localhost'`) — changed to `'*'` so it accepts connections on all interfaces, not just from the host itself.
3. **Firewall (UFW)**: even after Postgres was listening broadly, the connection still timed out. UFW's default policy was `deny (incoming)`. A rule was added scoped specifically to the Docker network's subnet rather than opening the port to everyone.
4. **Wrong subnet (the actual bug)**: the first UFW rule allowed `172.17.0.0/16`, assumed from the `host.docker.internal` resolution — but `docker network inspect` showed the actual Compose network (`day09_default`) used `172.18.0.0/16`. The firewall rule was allowing traffic from a network the containers weren't even on.
5. **pg_hba.conf**: after fixing the UFW subnet, the connection reached PostgreSQL but was rejected at the authentication layer — `pg_hba.conf` still had the old (wrong) `172.17.0.0/16` entry and needed updating to `172.18.0.0/16` to match.

### Object Storage integration

The second part introduced a new network path:

```text
Airflow worker
      |
      v
RustFS (S3 API)
      |
      v
raw/orders.csv
```

RustFS was connected to the Airflow Docker network so that the worker could reach it through:

```
http://rustfs:9000
```

The same ingestion code was first tested directly and then executed inside the Airflow worker. This verified that the pipeline was not only working from the host machine, but also from the actual Airflow task environment.

## What I Learned

- Airflow's job is not to transform data — it's to decide when tasks run, in what order, and what happens on failure. The actual work (reading a file, cleaning rows, calling dbt) stays in plain Python/SQL/dbt; Airflow just orchestrates it.
- `>>` between tasks defines a dependency graph, not just a visual order — a failed upstream task causes downstream tasks to be marked `upstream_failed` and skipped entirely, rather than running on incomplete data.
- `retries` + `retry_delay` are part of Airflow's failure-handling model, not just a config toggle — a task exhausts its retries before being marked `FAILED`, and only then does failure propagate downstream.
- Never assume an IP or hostname resolved during debugging is "the right one" — `host.docker.internal` resolving to *an* address didn't mean it was the address the actual Docker Compose network used. `docker network inspect <network>` gave the real subnet.
- A network/connection problem like this has independent layers (DNS resolution, firewall, service bind address, application-level auth) that can each fail separately. Testing them one at a time — hostname resolves? port reachable? firewall allows it? does the app-level auth accept it? — isolates the actual cause instead of changing multiple things and guessing which fix worked.
- Airflow Connections (`postgres_host`, set up in the UI) keep credentials out of DAG code — the DAG references a connection ID, not a password.
- dbt and Airflow solve different problems and compose together rather than overlap: dbt owns the SQL transformation logic and its dependency graph between *models*; Airflow owns the scheduling and dependency graph between *pipeline steps*, one of which can be "run dbt."
- Object storage can act as the raw-data layer before relational transformation — in this case, RustFS stored the raw CSV while PostgreSQL stored the processed relational data.
- An S3-compatible API allows Python code to interact with local object storage using the same general client model used for cloud object storage.
- Incremental processing and idempotency solve different problems: the watermark decides what should be processed, while the UPSERT determines what happens if the same record is processed again.
- The same pipeline should be tested both directly and from its real orchestration environment — code working on the host does not automatically mean it will work inside an Airflow worker container.
- Removing a stray `ingest()` call from module-level code mattered: Airflow parses DAG files periodically regardless of whether a run is triggered, so any code that executes at import time (rather than only inside a task) runs silently and repeatedly in the background.