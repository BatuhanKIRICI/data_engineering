"""
09 - Partitions: repartition vs coalesce

What this file shows:
  - repartition(n) does a full shuffle (Exchange) and can increase or decrease n.
  - coalesce(n) only merges existing partitions (no Exchange) and cannot increase n.
  - Anti-pattern: repartition(n) right before groupBy adds an EXTRA shuffle,
    because groupBy needs its own hash shuffle anyway.
  - Small-files note: many tiny partitions -> many tiny output files.
    coalesce(n) before a write is the usual cheap fix.

Machine-dependent values (e.g. the original partition count depends on CPU
cores) are printed, NOT asserted.

# Tested with pyspark 4.2.0, Java 17
"""

import contextlib
import io

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("HandbookPartitions").master("local[*]").getOrCreate()
)


def plan_text(dataframe):
    """Capture explain() output as a string so we can assert on it."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        dataframe.explain()
    return buffer.getvalue()


orders = [
    (101, "Ali", 120),
    (102, "Ayse", 80),
    (103, "Ali", 200),
    (104, "Mehmet", 150),
    (105, "Ali", 90),
    (106, "Ayse", 170),
]

df = spark.createDataFrame(orders, ["order_id", "customer", "amount"])
original_partitions = df.rdd.getNumPartitions()

print("\n1. Original partition count (machine-dependent):")
print(original_partitions)

# --- repartition(2): full shuffle ------------------------------------------
repartitioned_df = df.repartition(2)

print("\n2. After repartition(2):")
print(repartitioned_df.rdd.getNumPartitions())
assert repartitioned_df.rdd.getNumPartitions() == 2

# How many rows landed in each partition? (exact split is not guaranteed)
distribution = {
    row["pid"]: row["n"]
    for row in repartitioned_df.withColumn("pid", F.spark_partition_id())
    .groupBy("pid")
    .agg(F.count("*").alias("n"))
    .collect()
}
print("\n3. Rows per partition:", distribution)
assert sum(distribution.values()) == 6  # no row lost or duplicated
assert len(distribution) <= 2  # at most 2 non-empty partitions

# --- repartition + groupBy: the anti-pattern -------------------------------
totals_df = repartitioned_df.groupBy("customer").agg(
    F.sum("amount").alias("total_amount")
)

print("\n4. Customer totals:")
totals_df.show()

print("\n5. Execution plan (look for TWO Exchange nodes):")
totals_df.explain()
# Exchange #1: RoundRobinPartitioning(2)  <- from our repartition(2)
# Exchange #2: hashpartitioning(customer, ...) <- groupBy needs it anyway
# The first one was wasted work. Plan shape: partial_sum -> Exchange -> sum.
assert plan_text(totals_df).count("Exchange") >= 2

results = {row["customer"]: row["total_amount"] for row in totals_df.collect()}
assert results == {"Ali": 410, "Ayse": 250, "Mehmet": 150}

print("\nPartition and shuffle checks passed!")

# --- coalesce: no shuffle ---------------------------------------------------
# A) chained after repartition(4)
four_partitions_df = df.repartition(4)
two_chained_df = four_partitions_df.coalesce(2)

print("\n6. Repartition(4) then coalesce(2):")
print("Repartition:", four_partitions_df.rdd.getNumPartitions())
print("Coalesce:", two_chained_df.rdd.getNumPartitions())
assert four_partitions_df.rdd.getNumPartitions() == 4
assert two_chained_df.rdd.getNumPartitions() == 2

# B) directly on the original DataFrame
two_direct_df = df.coalesce(2)

print("\n7. Direct coalesce(2) plan (expect Coalesce, NO Exchange):")
two_direct_df.explain()

direct_plan = plan_text(two_direct_df)
assert "Coalesce" in direct_plan
assert "Exchange" not in direct_plan  # coalesce itself does not shuffle
# Note: min() because coalesce can never end up with MORE than it started with.
assert two_direct_df.rdd.getNumPartitions() == min(2, original_partitions)
assert two_direct_df.count() == 6  # data preserved

# C) coalesce cannot increase the partition count
too_many_df = df.coalesce(original_partitions + 10)
print("\n8. coalesce(more than current):", too_many_df.rdd.getNumPartitions())
assert too_many_df.rdd.getNumPartitions() == original_partitions

print("\nRepartition and coalesce checks passed!")

spark.stop()
