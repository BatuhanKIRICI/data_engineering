# ============================================================
# PySpark 01: SparkSession, DataFrame, select, filter, withColumn, groupBy/agg
#
# Problem:
#   Build a small Spark DataFrame of orders and do the same basic work we did
#   in SQL and pandas: pick columns, filter rows, add a column, aggregate
#   per customer.
#
# Pattern:
#   SparkSession.builder.appName(..).master("local[*]").getOrCreate()
#   spark.createDataFrame(rows, [column names])
#   df.select("a", "b")                           columns
#   df.filter(df["amount"] > 100)                 rows
#   df.withColumn("new", expr)                    new column (df stays unchanged)
#   df.groupBy("customer").agg(F.sum(..).alias(..), ...)   several metrics
#   .orderBy(..)                                  order is NOT guaranteed otherwise
#
# Verified (assert):
#   - schema: 4 rows, 3 columns, types bigint / string / bigint
#   - select keeps only the requested columns
#   - filter amount > 100 returns exactly orders 101, 103, 104
#   - withColumn adds amount_with_tax and leaves the original df unchanged
#   - amount * 1.10 gives 220.00000000000003 for amount 200 (float), so the
#     value check uses round(.., 2)
#   - per-customer totals: Ali 320, Ayse 80, Mehmet 150; grand total 550
#   - multi-metric summary: total / count / average for all three customers
#
# NOT covered:
#   - DecimalType for money (round hides the float error, it does not fix it)
#   - null customers in groupBy (Spark keeps null as its own group)
#   - lazy evaluation in detail, partitions, performance, large data
#
# Notes:
#   * show() and count() are actions: they run the work. select / filter /
#     withColumn only describe it.
#   * collect() pulls all rows to the driver: fine for 4 rows, dangerous for
#     big data.
#   * Row order is not guaranteed without orderBy, so checks sort first.
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("HandbookPySpark").master("local[*]").getOrCreate()

orders = [
    (101, "Ali", 120),
    (102, "Ayse", 80),
    (103, "Ali", 200),
    (104, "Mehmet", 150),
]

df = spark.createDataFrame(orders, ["order_id", "customer", "amount"])

# --- 1. DataFrame and schema ---
print("\nOrders:")
df.show()

print("\nSchema:")
df.printSchema()

assert df.count() == 4
assert df.columns == ["order_id", "customer", "amount"]
assert dict(df.dtypes) == {
    "order_id": "bigint",
    "customer": "string",
    "amount": "bigint",
}

# --- 2. select and filter ---
selected_df = df.select("order_id", "amount")

print("\nSelected columns:")
selected_df.show()

large_orders = df.filter(df["amount"] > 100)

print("\nOrders above 100:")
large_orders.show()

assert selected_df.columns == ["order_id", "amount"]
assert large_orders.count() == 3
assert sorted(row["order_id"] for row in large_orders.collect()) == [101, 103, 104]

# --- 3. withColumn: new column, original stays unchanged ---
# Plain amount * 1.10 gives 220.00000000000003 for amount 200 (float precision),
# so we round to 2 decimals. For real money use DecimalType.
taxed_orders = df.withColumn("amount_with_tax", F.round(df["amount"] * 1.10, 2))

print("\nOrders with tax:")
taxed_orders.show()

assert "amount_with_tax" in taxed_orders.columns
assert "amount_with_tax" not in df.columns
assert taxed_orders.count() == df.count()
assert [
    row["amount_with_tax"] for row in taxed_orders.orderBy("order_id").collect()
] == [
    132.0,
    88.0,
    220.0,
    165.0,
]

# --- 4. groupBy: one metric per customer ---
customer_totals = df.groupBy("customer").agg(F.sum("amount").alias("total_amount"))

print("\nCustomer totals:")
customer_totals.orderBy("customer").show()

totals = {row["customer"]: row["total_amount"] for row in customer_totals.collect()}

assert totals == {"Ali": 320, "Ayse": 80, "Mehmet": 150}
assert sum(totals.values()) == 550  # hand-calculated, same as the source total

# --- 5. agg: several metrics at once ---
customer_summary = (
    df.groupBy("customer")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("order_id").alias("order_count"),
        F.avg("amount").alias("average_amount"),
    )
    .orderBy("customer")
)

print("\nCustomer summary:")
customer_summary.show()

summary = {
    row["customer"]: (row["total_amount"], row["order_count"], row["average_amount"])
    for row in customer_summary.collect()
}

assert summary == {
    "Ali": (320, 2, 160.0),
    "Ayse": (80, 1, 80.0),
    "Mehmet": (150, 1, 150.0),
}

print("\nAll PySpark basics checks passed!")

spark.stop()
