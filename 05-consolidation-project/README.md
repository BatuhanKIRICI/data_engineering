# Incremental Orders Pipeline — Consolidation Project

## Project Overview

This project is part of my Data Engineering learning journey. After several days of concept-heavy learning (Day 8-11: dbt, Airflow, idempotency, incremental processing, watermarks), this project consolidates everything into one small, actually-running pipeline instead of adding new theory on top of unpracticed concepts.

The goal: a Python script that loads orders from a CSV into PostgreSQL, safely, only processing new data on each run — before adding dbt and Airflow on top in later steps.

## Technologies

- Python (`psycopg2`, `python-dotenv`)
- PostgreSQL

## What I Built

### Pipeline (current state)
```
orders.csv
    |
    v
ingest.py
    |
    +-- reads pipeline_state.last_processed_at (the watermark)
    |
    +-- filters CSV rows: only created_at > watermark are processed
    |
    +-- INSERT ... ON CONFLICT (order_id) DO UPDATE
    |       (idempotent: safe to insert the same order_id again)
    |
    v
consolidation_orders (PostgreSQL)
    |
    +-- after loading, computes MAX(created_at) from the table
    |
    v
pipeline_state (watermark updated, only if new rows were processed)
```

### Tables
```
consolidation_orders
---------------------
order_id      PK
customer
created_at
amount

pipeline_state
---------------------
pipeline_name PK
last_processed_at
```

### Secrets
The database password is read from a `.env` file (`POSTGRES_PASSWORD`) via `python-dotenv`, rather than hardcoded in `ingest.py`.

## Tests Actually Run (not just theory)

1. **First run** (no watermark yet): all 5 rows loaded, watermark set to the latest `created_at` in the data.
2. **Second run, no new data**: all rows correctly skipped (watermark equals the newest row's date), watermark left unchanged.
3. **New row added** (`106,Anna,2026-09-20,150`): only that one row processed; the previous 5 were skipped by the incremental filter — confirmed by row count staying at 6 (no duplicates) after running.
4. **Re-running with no changes**: idempotency confirmed — running the script again produces the same 6 rows, not 12.

## What I Learned

- Building the same concepts hands-on (rather than only discussing them) surfaced gaps that pure discussion didn't — e.g. `INSERT ... ON CONFLICT ... DO UPDATE` syntax wasn't something to derive from first principles; it needed to be shown once, run, and understood from the result.
- The watermark is read once at the start of a run and only written back *after* a successful load — and only when something was actually processed, to avoid a pointless write when there's nothing new.
- Incremental filtering (`created_at > watermark`) and idempotency (`ON CONFLICT DO UPDATE`) are independent safety nets: the filter decides what *should* be reprocessed, but idempotency is what keeps a mistake in that filter (or a retry) from actually corrupting the data.
- A working small pipeline, verified with real before/after row counts, is worth more at this stage than more new theory — this project exists specifically to close that gap.

## Next Steps
- Connect this pipeline's output to dbt models (`dbt run` + `dbt test` after ingestion)
- Wrap the whole thing in an Airflow DAG (`ingest` -> `dbt run` -> `dbt test`)