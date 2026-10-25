# Tested with pyspark 4.2.0, Java 17
from pathlib import Path
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HandbookFileIO").master("local[*]").getOrCreate()

base_dir = Path(__file__).resolve().parent

csv_path = base_dir / "data" / "orders.csv"
parquet_path = base_dir / "data" / "orders_parquet"

df = spark.read.option("header", True).option("inferSchema", True).csv(str(csv_path))

print("\nCSV data:")
df.show()

print("\nCSV schema:")
df.printSchema()

df.write.mode("overwrite").parquet(str(parquet_path))

parquet_df = spark.read.parquet(str(parquet_path))

print("\nParquet data:")
parquet_df.show()

print("\nParquet schema:")
parquet_df.printSchema()


def rows(frame):
    return sorted((r["order_id"], r["customer"], r["amount"]) for r in frame.collect())


expected = [
    (101, "Ali", 120),
    (102, "Ayse", 80),
    (103, "Ali", 200),
    (104, "Mehmet", 150),
]

assert rows(df) == expected
assert rows(parquet_df) == expected
assert dict(parquet_df.dtypes) == dict(df.dtypes)
assert parquet_df.agg({"amount": "sum"}).first()[0] == 550

print("\nFile IO checks passed!")

spark.stop()
