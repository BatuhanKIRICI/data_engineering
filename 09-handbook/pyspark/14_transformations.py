# Tested with pyspark 4.2.0, Java 17
"""
14 - Data cleaning and transformations

What this file shows:
  - trim / upper     : clean text
  - cast             : string -> int
  - to_date          : string -> date
  - F.coalesce       : first non-null value (NOT df.coalesce(n), which
                       reduces partitions)
  - when / otherwise : CASE WHEN
  - Bad values       : with ANSI mode on (Spark 4 default) a plain cast of
                       "abc" to int RAISES; try_cast returns NULL instead.
  - Real pipelines   : don't silently turn missing amounts into 0. Split
                       invalid rows into their own table (invalid_records).
                       The coalesce(..., 0) below only demonstrates the function.
"""

import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("HandbookTransformations")
    .master("local[*]")
    .getOrCreate()
)

orders = [
    (101, " ali ", "120", "2026-10-23"),
    (102, "AYSE", "80", "2026-10-23"),
    (103, " mehmet ", None, "2026-10-24"),
    (104, "Ali", "200", "2026-10-24"),
]

df = spark.createDataFrame(orders, ["order_id", "customer", "amount", "order_date"])

print("\n1. Raw data:")
df.show()
df.printSchema()

# --- Clean text, convert types, handle missing values -------------------------
clean_df = (
    df.withColumn("customer", F.upper(F.trim(F.col("customer"))))
    .withColumn("amount", F.coalesce(F.col("amount").cast("int"), F.lit(0)))
    .withColumn("order_date", F.to_date(F.col("order_date"), "yyyy-MM-dd"))
    .withColumn(
        "amount_category",
        F.when(F.col("amount") >= 150, "high").otherwise("low"),
    )
)

print("\n2. Cleaned data:")
clean_df.orderBy("order_id").show()

print("\n3. Cleaned schema:")
clean_df.printSchema()

rows = {row["order_id"]: row for row in clean_df.collect()}

assert {k: r["customer"] for k, r in rows.items()} == {
    101: "ALI",
    102: "AYSE",
    103: "MEHMET",
    104: "ALI",
}
assert {k: r["amount"] for k, r in rows.items()} == {
    101: 120,
    102: 80,
    103: 0,
    104: 200,
}
assert {k: r["amount_category"] for k, r in rows.items()} == {
    101: "low",
    102: "low",
    103: "low",
    104: "high",
}
assert rows[101]["order_date"] == datetime.date(2026, 10, 23)

types = dict(clean_df.dtypes)
assert types["amount"] == "int"
assert types["order_date"] == "date"
assert clean_df.schema["amount"].nullable is False  # coalesce with lit(0)

print("\nCleaning checks passed!")

# --- Bad values: strict cast vs try_cast --------------------------------------
dirty_df = spark.createDataFrame(
    [(201, "50"), (202, "abc"), (203, None)],
    ["order_id", "amount"],
)

ansi_on = spark.conf.get("spark.sql.ansi.enabled").lower() == "true"
print("\n4. ANSI mode enabled:", ansi_on)

strict_df = dirty_df.withColumn("amount_int", F.col("amount").cast("int"))
strict_failed = False
try:
    strict_df.collect()
except Exception as error:
    strict_failed = True
    print("Strict cast failed on 'abc':", type(error).__name__)

# With ANSI on the bad value must raise; with ANSI off it becomes NULL.
assert strict_failed == ansi_on

safe_df = dirty_df.withColumn("amount_int", F.expr("try_cast(amount as int)"))

print("\n5. try_cast keeps going (bad value -> NULL):")
safe_df.orderBy("order_id").show()

safe = {row["order_id"]: row["amount_int"] for row in safe_df.collect()}
assert safe == {201: 50, 202: None, 203: None}

# --- Split valid and invalid rows ---------------------------------------------
valid_df = safe_df.filter(F.col("amount_int").isNotNull())
invalid_df = safe_df.filter(F.col("amount_int").isNull())

print("\n6. invalid_records (NULL or not a number):")
invalid_df.orderBy("order_id").show()

assert valid_df.count() == 1
assert {row["order_id"] for row in invalid_df.collect()} == {202, 203}
assert valid_df.count() + invalid_df.count() == dirty_df.count()  # nothing lost

print("\nBad-value checks passed!")

spark.stop()
