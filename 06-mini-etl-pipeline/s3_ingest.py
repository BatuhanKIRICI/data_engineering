import boto3
import csv
from datetime import date
from io import StringIO
import psycopg2
import os

from dotenv import load_dotenv

from botocore.config import Config

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.environ["RUSTFS_ACCESS_KEY"],
    aws_secret_access_key=os.environ["RUSTFS_SECRET_KEY"],
    region_name="us-east-1",
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
    ),
)


bucket = "mini-data-lake"
object_key = "raw/orders.csv"


response = s3.get_object(
    Bucket=bucket,
    Key=object_key,
)

data = response["Body"].read().decode("utf-8")


reader = csv.DictReader(StringIO(data))

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgres",
    user="batuhan",
    password=os.getenv("POSTGRES_PASSWORD"),
)

cursor = conn.cursor()

for row in reader:
    order_id = int(row["order_id"])
    customer = row["customer"]
    created_at = date.fromisoformat(row["created_at"])
    amount = float(row["amount"])

    print(
        order_id,
        customer,
        created_at,
        amount,
    )

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


conn.commit()

cursor.close()
conn.close()

print("orders loaded successfully!")
