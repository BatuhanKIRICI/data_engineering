# dbt — Introduction, Models, ref(), and Tests — Day 8

## Project Overview

This project is part of my Data Engineering learning journey. The goal of Day 8 was to understand what dbt actually solves — not learning dbt's full feature set, but building the mental model of how it organizes and manages SQL-based transformations on top of PostgreSQL.

## Technologies

- dbt-core + dbt-postgres (in a dedicated `.venv-dbt` virtual environment)
- PostgreSQL

## What I Built

### Setup
- Created an isolated Python virtual environment (`.venv-dbt`) so dbt's dependencies don't mix with other project environments
- Installed `dbt-core` and the `dbt-postgres` adapter
- Ran `dbt init`, connecting to the existing `postgres` database / `public` schema (the same database `fact_sales`, `dim_customer`, etc. already live in)
- Verified the connection with `dbt debug` (`Connection test: [OK connection ok]`)

### Model chain
```
fact_sales + dim_customer
        |
        v
revenue_per_customer   (JOIN + GROUP BY + SUM)
        |
        v (ref())
segment_per_customer   (CASE WHEN -> VIP / Premium / Standard)
```

- **`revenue_per_customer`** — joins `fact_sales` to `dim_customer` directly (raw table names, no `source()` yet), grouping by customer to get total revenue per customer.
- **`segment_per_customer`** — built entirely on top of `revenue_per_customer` using `{{ ref('revenue_per_customer') }}`, applying a `CASE WHEN` to bucket customers into VIP (>=1000), Premium (>=500), or Standard.

### Tests
Defined in `models/marts/schema.yml`:

| Column | Test |
|---|---|
| `customer_id` | `unique`, `not_null` |
| `customer_name` | `not_null` |
| `segment` | `not_null` |

`dbt test` result: **4/4 PASS**.

## What I Learned

- dbt is not a replacement for SQL and not a BI/analysis tool — it's a transformation layer that organizes, chains, and tests SQL that runs against an existing database (database sources -> dbt transformations -> analytics-ready models -> BI/analytics).
- The problem dbt solves isn't "how do I write SQL" but "how do I manage many interdependent SQL transformations" — dependency tracking, change impact, testing, and lineage (tracing a bad number in a report back to its source) all get harder without it once there's more than a couple of models.
- `dbt run` executes a model's SQL against the database and materializes it (as a view, by default) — it proves the model executed successfully, but not that the resulting data is logically correct.
- `{{ ref('other_model') }}` tells dbt "this model depends on that other dbt model." dbt uses this to automatically figure out execution order — I never had to specify "run revenue_per_customer before segment_per_customer" manually; dbt inferred it from the `ref()` call.
- There's a difference between dbt's connection to the database (`profiles.yml`, set up once via `dbt init`) and how a model references a raw table (currently: writing the table name directly, e.g. `from fact_sales`) versus another dbt model (`ref()`). Referencing raw source tables through `source()` instead of bare table names is a more production-style pattern, not covered yet.
- `dbt test` is separate from `dbt run`: tests check whether the *data* meets expectations (uniqueness, non-null columns), not whether the SQL executed. A model can run successfully and still produce data that fails its tests.
- Not every column needs a `not_null` test — only ones where a NULL genuinely signals a problem. A column protected by a SQL `CASE ... ELSE ...` (like `segment`) is a good `not_null` candidate because the logic already guarantees it won't be empty; the test is a safety net in case that logic changes later.
