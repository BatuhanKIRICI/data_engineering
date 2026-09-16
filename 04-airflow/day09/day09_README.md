# Airflow + Docker — First Orchestration Pipeline — Day 9

## Project Overview

This project is part of my Data Engineering learning journey. The goal of Day 9 was to move from "SQL/dbt know *how* to transform data" to "Airflow knows *when and in what order* things run" — building a first real DAG that reads a CSV, cleans it, and loads it into PostgreSQL, including retry and failure-handling behavior.

## Technologies

- Apache Airflow 3.3.1 (Docker Compose, official quick-start setup)
- Docker / Docker Compose
- PostgreSQL (host machine)
- Python (PythonOperator)

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
- 8 raw rows in → 6 valid rows out (1 dropped for a malformed column count, 1 dropped for a missing `customer` value)
- `load` uses `PostgresHook` with an Airflow Connection (`postgres_host`) rather than a hardcoded password in the DAG code

### Failure/retry test
Deliberately raised an exception inside `extract()` to observe real failure propagation:
```
extract  -> FAILED (after exhausting its configured retries)
clean    -> upstream_failed (never ran)
load     -> upstream_failed (never ran)
```
This confirmed that Airflow's dependency graph doesn't just define order — it prevents downstream tasks from running on top of a failed upstream step, rather than silently continuing with missing or bad data.

### dbt integration — scope decision
Chose *not* to run dbt from inside the Airflow container this round (would require installing dbt into the Airflow image or a separate execution environment — real complexity, low learning payoff for a first DAG). The conceptual role is clear instead: Airflow decides *when* things run and *in what order*; dbt decides *how* the SQL transformation itself is structured. In a fuller pipeline, a bash/dbt operator task would sit after `load`, calling `dbt run` against the already-loaded data.

## The Debugging Journey (the real lesson of the day)

Getting the Airflow container to talk to PostgreSQL on the host turned into a multi-layer networking problem, solved one layer at a time instead of changing several things at once:

1. **DNS**: `host.docker.internal` isn't automatically available on native Docker Engine (Linux) the way it is on Docker Desktop (macOS/Windows) — fixed by adding `extra_hosts: ["host.docker.internal:host-gateway"]` to the Airflow services in `docker-compose.yaml`.
2. **PostgreSQL bind address**: Postgres was only listening on `localhost` (`listen_addresses = 'localhost'`) — changed to `'*'` so it accepts connections on all interfaces, not just from the host itself.
3. **Firewall (UFW)**: even after Postgres was listening broadly, the connection still timed out. UFW's default policy was `deny (incoming)`. A rule was added scoped specifically to the Docker network's subnet rather than opening the port to everyone.
4. **Wrong subnet (the actual bug)**: the first UFW rule allowed `172.17.0.0/16`, assumed from the `host.docker.internal` resolution — but `docker network inspect` showed the actual Compose network (`day09_default`) used `172.18.0.0/16`. The firewall rule was allowing traffic from a network the containers weren't even on.
5. **pg_hba.conf**: after fixing the UFW subnet, the connection reached PostgreSQL but was rejected at the authentication layer — `pg_hba.conf` still had the old (wrong) `172.17.0.0/16` entry and needed updating to `172.18.0.0/16` to match.

## What I Learned

- Airflow's job is not to transform data — it's to decide when tasks run, in what order, and what happens on failure. The actual work (reading a file, cleaning rows, calling dbt) stays in plain Python/SQL/dbt; Airflow just orchestrates it.
- `>>` between tasks defines a dependency graph, not just a visual order — a failed upstream task causes downstream tasks to be marked `upstream_failed` and skipped entirely, rather than running on incomplete data.
- `retries` + `retry_delay` are part of Airflow's failure-handling model, not just a config toggle — a task exhausts its retries before being marked `FAILED`, and only then does failure propagate downstream.
- Never assume an IP or hostname resolved during debugging is "the right one" — `host.docker.internal` resolving to *an* address didn't mean it was the address the actual Docker Compose network used. `docker network inspect <network>` gave the real subnet.
- A network/connection problem like this has independent layers (DNS resolution, firewall, service bind address, application-level auth) that can each fail separately. Testing them one at a time — hostname resolves? port reachable? firewall allows it? does the app-level auth accept it? — isolates the actual cause instead of changing multiple things and guessing which fix worked.
- Airflow Connections (`postgres_host`, set up in the UI) keep credentials out of DAG code — the DAG references a connection ID, not a password.
- dbt and Airflow solve different problems and compose together rather than overlap: dbt owns the SQL transformation logic and its dependency graph between *models*; Airflow owns the scheduling and dependency graph between *pipeline steps*, one of which can be "run dbt."
