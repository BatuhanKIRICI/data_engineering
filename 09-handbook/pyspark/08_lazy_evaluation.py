# Tested with pyspark 4.2.0, Java 17
# ============================================================
# PySpark 08: lazy evaluation, transformations vs actions, explain()
#
# Problem:
#   Understand when Spark actually does the work, and how to read the plan
#   it builds.
#
# Pattern:
#   transformations (build a plan, do not compute): filter, select, withColumn
#   actions (run the plan):                          show, count, collect
#   df.explain()        print the physical plan (does not run the query)
#   df.explain(True)    parsed / analyzed / optimized / physical plans
#
# Verified (assert):
#   - the filtered result is orders 101, 103, 104 with columns order_id, amount
#   - laziness: a divide-by-zero in withColumn raises no error when the
#     transformation is defined, only when an action runs
#
# NOT covered:
#   - the exact text of the plans (printed, not asserted: it changes between
#     Spark versions)
#   - stages, shuffles, caching, the Spark UI
#   - why the divide-by-zero fails: Spark 4 runs with ANSI mode on by default.
#     With ANSI off Spark returns NULL instead and the laziness check below
#     would fail.
#
# Notes:
#   * Read a plan bottom to top: the last line is the first step.
#   * Spark added isnotnull(amount) to the filter itself: NULL > 100 is never
#     true, so null rows can be dropped early.
#   * A select that keeps every column in the same order is a no-op, so the
#     optimizer removes it (no Project in the plan).
#   * Every action runs the plan again. count() after show() recomputes.
#   * With Parquet, selected columns go into ReadSchema and the filter into
#     PushedFilters: Spark does not read the other columns from disk.
# ============================================================

import tempfile
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("HandbookLazyEvaluation")
    .master("local[*]")
    .getOrCreate()
)

orders = [
    (101, "Ali", 120),
    (102, "Ayse", 80),
    (103, "Ali", 200),
    (104, "Mehmet", 150),
]

df = spark.createDataFrame(orders, ["order_id", "customer", "amount"])

print("\n1. DataFrame created")

# --- 2. Transformations: only a plan is built, nothing is computed ---
filtered_df = df.filter(F.col("amount") > 100)
selected_df = filtered_df.select("order_id", "amount")

print("\n2. Transformations defined")

# --- 3. The plan (explain() is not an action) ---
print("\n3. Execution plan (in-memory data):")
selected_df.explain()

# --- 4. Actions: now Spark computes ---
print("\n4. Action: show()")
selected_df.show()

print("\n5. Action: count()")
row_count = selected_df.count()

assert row_count == 3
assert selected_df.columns == ["order_id", "amount"]
assert sorted(row["order_id"] for row in selected_df.collect()) == [101, 103, 104]

# --- 6. Proof of laziness: the error appears only when an action runs ---
lazy_df = df.withColumn("ratio", F.col("amount") / 0)  # no error here

print("\n6. withColumn(amount / 0) defined: no error yet")

try:
    lazy_df.collect()  # the action: the error happens now
except Exception:
    print("The error appeared only when the action ran.")
else:
    raise AssertionError("expected a divide-by-zero error from the action")

# --- 7. The plan on a Parquet file: pushed filters and read schema ---
parquet_dir = Path(tempfile.mkdtemp()) / "orders_parquet"
df.write.mode("overwrite").parquet(str(parquet_dir))

parquet_df = spark.read.parquet(str(parquet_dir))
parquet_plan_df = parquet_df.filter(F.col("amount") > 100).select("order_id", "amount")

print("\n7. Execution plan (Parquet file):")
parquet_plan_df.explain()

assert sorted(row["order_id"] for row in parquet_plan_df.collect()) == [101, 103, 104]

print("\nAll lazy evaluation checks passed!")

spark.stop()
