import boto3
import pandas as pd
from io import BytesIO
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
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

df.to_sql(
    "weather_data",
    engine,
    if_exists="replace",
    index=False,
)

print("Loaded to PostgreSQL.")
