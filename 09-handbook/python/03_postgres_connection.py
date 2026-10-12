import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

connection = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="chinook",
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
)

cursor = None

try:
    print("Connected to PostgreSQL!")
    cursor = connection.cursor()

    cursor.execute("select count(*) from customer")
    customer_count = cursor.fetchone()[0]
    print("Customer count:", customer_count)

    cursor.execute("""
        select customer_id, first_name, last_name
        from customer
        order by customer_id
        limit 5
    """)
    customers = cursor.fetchall()

    print("\nFirst 5 customers:")
    for customer in customers:
        print(customer)

finally:
    if cursor is not None:
        cursor.close()
    connection.close()
    print("Connection closed.")
