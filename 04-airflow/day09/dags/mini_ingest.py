import csv
import os
from datetime import date
from io import StringIO

import boto3
import psycopg2
from botocore.config import Config
from dotenv import load_dotenv


def ingest():
    load_dotenv()

    s3 = boto3.client(
        "s3",
        endpoint_url="http://rustfs:9000",
        aws_access_key_id=os.getenv("RUSTFS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("RUSTFS_SECRET_KEY"),
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )

    response = s3.get_object(
        Bucket="mini-data-lake",
        Key="raw/orders.csv",
    )

    data = response["Body"].read().decode("utf-8")
    reader = csv.DictReader(StringIO(data))

    conn = psycopg2.connect(
        host="host.docker.internal",
        port=5432,
        database="postgres",
        user="batuhan",
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    cursor = conn.cursor()

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

    for row in reader:
        order_id = int(row["order_id"])
        customer = row["customer"]
        created_at = date.fromisoformat(row["created_at"])
        amount = float(row["amount"])

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
                order_id,
                customer,
                created_at,
                amount,
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
