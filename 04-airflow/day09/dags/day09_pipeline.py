from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extract():
    with open("/opt/airflow/dags/orders_dirty.csv", "r") as f:
        data = f.read()

    print(data)


def clean():
    with open("/opt/airflow/dags/orders_dirty.csv", "r") as f:
        lines = f.readlines()

    header = lines[0].strip()
    data = [line.strip() for line in lines[1:] if line.strip()]

    print("Header:", header)
    print("Rows before cleaning:", len(data))

    cleaned_data = []

    for row in data:
        parts = row.split(",")

        if len(parts) != 4:
            continue

        order_id, customer, country, amount = parts

        if not order_id or not customer or not country or not amount:
            continue

        cleaned_data.append(row)

    output_path = "/opt/airflow/dags/orders_clean.csv"

    with open(output_path, "w") as f:
        f.write("order_id,customer,country,amount\n")

        for row in cleaned_data:
            f.write(row + "\n")

    print("Rows after cleaning:", len(cleaned_data))
    print(f"Cleaned data written to: {output_path}")


def load():
    csv_path = "/opt/airflow/dags/orders_clean.csv"

    hook = PostgresHook(
        postgres_conn_id="postgres_host"
    )

    create_sql = """
    CREATE TABLE IF NOT EXISTS orders_clean (
        order_id INT PRIMARY KEY,
        customer VARCHAR(50) NOT NULL,
        country VARCHAR(2) NOT NULL,
        amount NUMERIC(10,2) NOT NULL
    );
    """

    hook.run(create_sql)

    hook.run("TRUNCATE TABLE orders_clean")

    rows = []

    with open(csv_path, "r") as f:
        next(f)

        for line in f:
            order_id, customer, country, amount = line.strip().split(",")

            rows.append(
                (
                    int(order_id),
                    customer,
                    country,
                    float(amount)
                )
            )

    hook.insert_rows(
        table="orders_clean",
        rows=rows,
        target_fields=[
            "order_id",
            "customer",
            "country",
            "amount"
        ],
    )

    print(f"Loaded {len(rows)} rows into orders_clean")


with DAG(
    dag_id="day09_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    clean_task = PythonOperator(
        task_id="clean",
        python_callable=clean,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    extract_task >> clean_task >> load_task