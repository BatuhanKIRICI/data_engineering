import csv
import os
from datetime import date

import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host="host.docker.internal",
    port=5432,
    database="postgres",
    user="batuhan",
    password=os.getenv("POSTGRES_PASSWORD"),
)

cursor = conn.cursor()

# read watermark
cursor.execute(
    """
    select last_processed_at
    from mini_pipeline_state
    where pipeline_name = %s
    """,
    ("mini_orders_pipeline",),
)

result = cursor.fetchone()

print("watermark:", result)

# read csv
with open("data/orders.csv", newline="") as file:
    reader = csv.DictReader(file)

    for row in reader:
        created_at = date.fromisoformat(row["created_at"])

        if result is not None and created_at <= result[0]:
            continue

    cursor.execute(
        """
        insert into mini_orders (
            order_id,
            customer,
            created_at,
            amount
        )
        values (%s, %s, %s, %s)
        on conflict (order_id)
        do update set
            customer = excluded.customer,
            created_at = excluded.created_at,
            amount = excluded.amount
        """,
        (
            row["order_id"],
            row["customer"],
            created_at,
            row["amount"],
        ),
    )

    cursor.execute("""
    select max(created_at)
    from mini_orders
    """)

max_date = cursor.fetchone()[0]

cursor.execute(
    """
    insert into mini_pipeline_state (
        pipeline_name,
        last_processed_at
    )
    values (%s, %s)
    on conflict (pipeline_name)
    do update set
        last_processed_at = excluded.last_processed_at
    """,
    ("mini_orders_pipeline", max_date),
)

conn.commit()

cursor.close()
conn.close()

print("orders loaded successfully!")
