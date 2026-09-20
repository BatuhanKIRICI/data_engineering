from datetime import date

import csv
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="postgres",
    user="batuhan",
    password=os.getenv("POSTGRES_PASSWORD"),
)

cursor = conn.cursor()

cursor.execute(
    """
    select last_processed_at
    from pipeline_state
    where pipeline_name = %s
    """,
    ("orders_pipeline",),
)

result = cursor.fetchone()

print("watermark:", result)

with open("data/orders.csv", "r") as f:
    reader = csv.DictReader(f)

    for row in reader:
        created_at = date.fromisoformat(row["created_at"])

        if result is not None and created_at <= result[0]:
            continue

        cursor.execute(
            """
            insert into consolidation_orders (
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
                int(row["order_id"]),
                row["customer"],
                row["created_at"],
                float(row["amount"]),
            ),
        )

        conn.commit()

cursor.execute("""
    select max(created_at)
    from consolidation_orders
    """)

max_date = cursor.fetchone()[0]

cursor.execute(
    """
    insert into pipeline_state (
        pipeline_name,
        last_processed_at
    )
    values (%s, %s)
    on conflict (pipeline_name)
    do update set
        last_processed_at = excluded.last_processed_at
    """,
    ("orders_pipeline", max_date),
)

conn.commit()

print("watermark updated to:", max_date)
print("Orders loaded successfully!")

cursor.close()
conn.close()
