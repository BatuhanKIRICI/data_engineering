# 08 — Weather Data Lake (S3 -> PostgreSQL)

## Goal

Move data from raw object storage into a relational database using a real-world dataset instead of a synthetic CSV.

The project was built independently from earlier projects with its own `.venv`, RustFS bucket, and `.env`.

## Dataset

Seattle daily weather data, 2012-2015 (1461 rows), downloaded from Kaggle.

Columns:

- `date`
- `precipitation`
- `temp_max`
- `temp_min`
- `wind`
- `weather`

## Pipeline

```text
seattle-weather.csv (local)
    |
    | aws s3 cp
    v
RustFS bucket: weather-lake
    -> raw/seattle-weather.csv
    |
    | boto3 get_object()
    v
bytes -> BytesIO -> pandas.read_csv()
    |
    v
DataFrame
    |
    | to_sql()
    v
PostgreSQL: weather_data
```

## Stack

- Python
- boto3
- pandas
- SQLAlchemy
- psycopg2
- PostgreSQL
- RustFS (S3-compatible object storage)
- AWS CLI

## What I Practiced

- Set up a new project independently with its own `.venv`, RustFS bucket, and `.env`
- Wrote the `aws s3 mb` and `aws s3 cp` commands independently, reusing what was learned on Cloud Day 1
- Read an S3 object with `boto3.get_object()`
- Converted raw object bytes into a pandas DataFrame using `BytesIO`
- Loaded the DataFrame into PostgreSQL using `pandas.to_sql()` and SQLAlchemy
- Kept credentials out of the code and loaded them from `.env` using `python-dotenv`
- Verified the loaded data with `count(*)` and `group by` queries using VS Code's PostgreSQL extension

## Scope for This Session

Only the extract -> load flow was implemented:

```text
object storage
    -> Python
    -> DataFrame
    -> PostgreSQL
```

SQL analytics, dbt models, and Airflow orchestration were deliberately left out to keep the project focused.

`to_sql(..., if_exists="replace")` overwrites the table on every run. Incremental loading was not part of this project because incremental processing and idempotency were already practiced in a previous project.

## Note

RustFS was used as a local, zero-cost S3-compatible object store instead of real AWS S3. This allowed the project to practice S3 concepts and APIs without depending on AWS usage or costs.
