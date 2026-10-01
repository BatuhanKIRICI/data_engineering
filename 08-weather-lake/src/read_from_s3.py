import boto3
import pandas as pd
from io import BytesIO
from sqlalchemy import create_engine
from sqlalchemy import text
import os
from dotenv import load_dotenv


def ingest():
    load_dotenv()

    s3 = boto3.client(
        "s3",
        endpoint_url="http://rustfs:9000",
        region_name=os.getenv("AWS_DEFAULT_REGION"),
    )

    response = s3.get_object(
        Bucket="weather-lake",
        Key="raw/seattle-weather.csv",
    )

    data = response["Body"].read()

    df = pd.read_csv(BytesIO(data))

    print(df.head())
    print(df.shape)

    engine = create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:"
        f"{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('POSTGRES_HOST')}:"
        f"{os.getenv('POSTGRES_PORT')}/"
        f"{os.getenv('POSTGRES_DB')}"
    )

    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE weather_data"))
        conn.commit()

    df.to_sql(
        "weather_data",
        engine,
        if_exists="append",
        index=False,
    )


print("Loaded to PostgreSQL.")
