# ============================================================
# PySpark 03: null values (isNull, fillna) and what they do to results
#
# Problem:
#   Orders arrive with a missing customer and a missing amount. Find them,
#   fill them, and see what filling does to totals and averages.
#
# Pattern:
#   df.filter(F.col("customer").isNull())       find missing values
#   df.fillna({"customer": "Unknown", "amount": 0})   fill per column
#   F.sum / F.avg skip nulls, fillna(0) makes them count
#
# Verified (assert):
#   - missing customer is order 102, missing amount is order 103
#   - fillna leaves no nulls in the filled DataFrame, and the original df
#     still has its null (fillna returns a new DataFrame)
#   - total stays 350 after fillna(0), but the average changes:
#     350 / 3 (nulls skipped) -> 350 / 4 = 87.5 (the 0 counts)
#   - "customer != 'Ali'" silently drops the null-customer row (order 102)
#
# NOT covered:
#   - NaN (float "not a number") vs null: isNull() does not catch NaN, use isnan()
#   - dropna(), coalesce(), na.replace(), filling from another column
#   - what to do with invalid rows (usually: move them to a separate table
#     instead of filling them, as in the pandas mini ETL)
#
# Notes:
#   * Spark prints null as NULL in show() (pandas showed NaN / None).
#   * Comparing with null gives "unknown", not True: != "Ali" does not return
#     rows where the customer is null. Add isNull() explicitly if you want them.
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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

# --- 1. Find missing values ---
missing_customers = df.filter(F.col("customer").isNull())
missing_amounts = df.filter(F.col("amount").isNull())

print("\nMissing customers:")
missing_customers.show()

print("\nMissing amounts:")
missing_amounts.show()

assert missing_customers.count() == 1
assert missing_customers.first()["order_id"] == 102
assert missing_amounts.count() == 1
assert missing_amounts.first()["order_id"] == 103

# --- 2. Fill missing values (returns a new DataFrame) ---
filled_df = df.fillna({"customer": "Unknown", "amount": 0})

print("\nFilled data:")
filled_df.orderBy("order_id").show()

assert filled_df.filter(F.col("customer").isNull()).count() == 0
assert filled_df.filter(F.col("amount").isNull()).count() == 0
assert df.filter(F.col("amount").isNull()).count() == 1  # original unchanged

# --- 3. What filling does to totals and averages ---
total_before = df.agg(F.sum("amount")).first()[0]
total_after = filled_df.agg(F.sum("amount")).first()[0]

assert total_before == total_after == 350  # the total does not change

avg_before = df.agg(F.avg("amount")).first()[0]
avg_after = filled_df.agg(F.avg("amount")).first()[0]

assert abs(avg_before - 350 / 3) < 1e-9  # null is skipped: 3 values
assert avg_after == 87.5  # the 0 counts: 4 values

# --- 4. Comparison with null silently drops rows ---
not_ali = df.filter(F.col("customer") != "Ali")

print("\ncustomer != 'Ali' (the null row is NOT included):")
not_ali.orderBy("order_id").show()

assert sorted(row["order_id"] for row in not_ali.collect()) == [
    103,
    104,
]  # 102 is missing

print("\nAll PySpark null checks passed!")

spark.stop()
