# Tested with pyspark 4.2.0, Java 17
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("HandbookNulls").master("local[*]").getOrCreate()

orders = [
    (101, "Ali", 120),
    (102, None, 80),
    (103, "Ayse", None),
    (104, "Mehmet", 150),
]

df = spark.createDataFrame(orders, ["order_id", "customer", "amount"])

print("\nOriginal data:")
df.show()

missing_customers = df.filter(F.col("customer").isNull())

missing_amounts = df.filter(F.col("amount").isNull())

print("\nMissing customers:")
missing_customers.show()

print("\nMissing amounts:")
missing_amounts.show()

filled_df = df.fillna({"customer": "Unknown", "amount": 0})

print("\nFilled data:")
filled_df.orderBy("order_id").show()

assert missing_customers.count() == 1
assert missing_amounts.count() == 1
assert filled_df.filter(F.col("customer").isNull()).count() == 0
assert filled_df.filter(F.col("amount").isNull()).count() == 0
assert df.filter(F.col("amount").isNull()).count() == 1

print("\nNull checks passed!")

spark.stop()
