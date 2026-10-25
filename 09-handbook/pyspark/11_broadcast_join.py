"""
11 - Broadcast join

What this file shows:
  - F.broadcast(small_df) ships the small table to every executor, so the
    big table is NOT shuffled (BroadcastHashJoin).
  - Spark also broadcasts automatically below spark.sql.autoBroadcastJoinThreshold
    (default 10MB), so on tiny demo data the plan alone does not prove the
    hint works. We set the threshold to -1 to prove it:
        no hint + threshold -1  -> SortMergeJoin
        hint    + threshold -1  -> BroadcastHashJoin
  - In a LEFT join only the right side can be broadcast.
  - Check the small table for duplicate keys first, or the join multiplies rows.

# Tested with pyspark 4.2.0, Java 17
"""

import contextlib
import io

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("HandbookBroadcastJoin")
    .master("local[*]")
    .getOrCreate()
)


def plan_text(dataframe):
    """Capture explain() output as a string so we can assert on it."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        dataframe.explain()
    return buffer.getvalue()


orders = [
    (101, 1, 120),
    (102, 2, 80),
    (103, 1, 200),
    (104, 3, 150),
    (105, 2, 170),
]

customers = [
    (1, "Ali"),
    (2, "Ayse"),
    (3, "Mehmet"),
]

orders_df = spark.createDataFrame(orders, ["order_id", "customer_id", "amount"])
customers_df = spark.createDataFrame(customers, ["customer_id", "customer_name"])

# Guard: the small side must have unique keys
assert customers_df.count() == customers_df.select("customer_id").distinct().count()

# --- 1. Hinted broadcast join ------------------------------------------------
joined_df = orders_df.join(F.broadcast(customers_df), on="customer_id", how="inner")

print("\n1. Broadcast join result:")
joined_df.orderBy("order_id").show()

print("\n2. Broadcast join execution plan:")
joined_df.explain()

assert joined_df.count() == 5
assert joined_df.agg(F.sum("amount")).first()[0] == 720
assert "BroadcastHashJoin" in plan_text(joined_df)

# --- 2. Prove the hint: disable automatic broadcasting -----------------------
original_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

try:
    plain_df = orders_df.join(customers_df, on="customer_id", how="inner")
    hinted_df = orders_df.join(F.broadcast(customers_df), on="customer_id", how="inner")

    print("\n3. Threshold = -1, NO hint (expect SortMergeJoin):")
    plain_df.explain()
    print("\n4. Threshold = -1, WITH hint (expect BroadcastHashJoin):")
    hinted_df.explain()

    assert "SortMergeJoin" in plan_text(plain_df)
    assert "BroadcastHashJoin" in plan_text(hinted_df)

    # Same answer, different strategy
    assert plain_df.agg(F.sum("amount")).first()[0] == 720
    assert hinted_df.agg(F.sum("amount")).first()[0] == 720
finally:
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", original_threshold)

# --- 3. Unmatched order: inner drops it, left keeps it -----------------------
orders_extra_df = spark.createDataFrame(
    orders + [(106, 4, 60)], ["order_id", "customer_id", "amount"]
)

inner_df = orders_extra_df.join(F.broadcast(customers_df), "customer_id", "inner")
left_df = orders_extra_df.join(F.broadcast(customers_df), "customer_id", "left")

print("\n5. Left join keeps the unmatched order (customer_name is NULL):")
left_df.orderBy("order_id").show()

assert inner_df.count() == 5
assert left_df.count() == 6
assert inner_df.agg(F.sum("amount")).first()[0] == 720  # order 106 dropped
assert left_df.agg(F.sum("amount")).first()[0] == 780  # 720 + 60

missing = left_df.filter(F.col("customer_name").isNull()).first()
assert missing["order_id"] == 106

print("\nBroadcast join checks passed!")

spark.stop()
