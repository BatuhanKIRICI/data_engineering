import pandas as pd
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

try:
    cursor = connection.cursor()

    cursor.execute("""
        select customer_id, first_name, last_name
        from customer
        order by customer_id
        limit 5
    """)

    rows = cursor.fetchall()

    column_names = [column[0] for column in cursor.description]

    db_df = pd.DataFrame(rows, columns=column_names)

    print("\nCustomers from PostgreSQL:")
    print(db_df)

    assert len(db_df) == 5
    assert list(db_df.columns) == ["customer_id", "first_name", "last_name"]

    print("DataFrame checks passed!")

finally:
    connection.close()
