# ============================================================
# PySpark 04: window functions (row_number, lag, lead)
#
# Problem:
#   Number each customer's orders by amount, pick the largest order per
#   customer, and look at the previous / next order inside the same group.
#
# Pattern:
#   w = Window.partitionBy("customer").orderBy(F.col("amount").desc(), F.col("order_id").asc())
#   df.withColumn("row_num", F.row_number().over(w))
#   df.withColumn("previous_amount", F.lag("amount", 1).over(w))
#   df.withColumn("next_amount",     F.lead("amount", 1).over(w))
#   ranked.filter(F.col("row_num") == 1)       one row per customer
#
# Verified (assert):
#   - row_number per (customer, order): Ali 103 -> 1, 101 -> 2, 105 -> 3;
#     Ayse 106 -> 1, 102 -> 2; Mehmet 104 -> 1
#   - top order per customer: Ali 103, Ayse 106, Mehmet 104 (a customer ->
#     order mapping, not just a count)
#   - lag / lead for all 6 orders; first row of a group has no previous
#     (None) and the last row has no next (None)
#
# NOT covered:
#   - ties on the sort key (see 05_ranking.py)
#   - lag / lead ordered by a date (the real use case)
#   - window frames (running totals), default value of lag / lead
#   - performance (a window shuffles data by the partition key)
#
# Notes:
#   * "partitionBy" here means "split into groups per customer" (like SQL
#     partition by). It is not Spark's data partitions across machines.
#   * order_id is only a tie-breaker; amount decides the order. That is why
#     the order ids look mixed inside a group.
#   * previous_amount means "one row up in THIS ordering" (amount descending),
#     not "earlier in time". For time, order by a date column.
#   * collect() pulls all rows to the driver: fine for 6 rows, not for big data.
# ============================================================

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("HandbookWindow").master("local[*]").getOrCreate()

orders = [
    (101, "Ali", 120),
    (102, "Ayse", 80),
    (103, "Ali", 200),
    (104, "Mehmet", 150),
    (105, "Ali", 90),
    (106, "Ayse", 170),
]

df = spark.createDataFrame(orders, ["order_id", "customer", "amount"])

# Groups per customer, largest amount first; order_id breaks ties
window_spec = Window.partitionBy("customer").orderBy(
    F.col("amount").desc(), F.col("order_id").asc()
)

# --- 1. row_number: number the orders inside each customer ---
ranked_df = df.withColumn("row_num", F.row_number().over(window_spec))

print("\nRanked orders:")
ranked_df.orderBy("customer", "row_num").show()

top_orders = ranked_df.filter(F.col("row_num") == 1)

print("\nTop order per customer:")
top_orders.orderBy("customer").show()

assert ranked_df.count() == 6
assert top_orders.count() == 3

numbers = {
    (row["customer"], row["order_id"]): row["row_num"] for row in ranked_df.collect()
}

assert numbers == {
    ("Ali", 103): 1,
    ("Ali", 101): 2,
    ("Ali", 105): 3,
    ("Ayse", 106): 1,
    ("Ayse", 102): 2,
    ("Mehmet", 104): 1,
}

best = {row["customer"]: row["order_id"] for row in top_orders.collect()}

assert best == {"Ali": 103, "Ayse": 106, "Mehmet": 104}

# --- 2. lag and lead: previous / next row inside the same group ---
comparison_df = df.withColumn(
    "previous_amount", F.lag("amount", 1).over(window_spec)
).withColumn("next_amount", F.lead("amount", 1).over(window_spec))

print("\nPrevious and next amounts:")
comparison_df.orderBy("customer", F.col("amount").desc()).show()

all_rows = {
    row["order_id"]: (row["previous_amount"], row["next_amount"])
    for row in comparison_df.collect()
}

assert all_rows == {
    103: (None, 120),  # Ali
    101: (200, 90),
    105: (120, None),
    106: (None, 80),  # Ayse
    102: (170, None),
    104: (None, None),  # Mehmet
}

assert comparison_df.count() == 6

print("\nAll PySpark window checks passed!")

spark.stop()
