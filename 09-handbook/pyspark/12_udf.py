"""
12 - Native functions vs Python UDF

What this file shows:
  - Same logic written twice: F.when(...) (native) vs a Python UDF.
  - Native runs inside the JVM and Spark can optimize it (plan: CASE WHEN).
    A UDF sends rows to a Python worker (plan: BatchEvalPython / ArrowEvalPython),
    which is slower and opaque to the optimizer.
  - NULL behaviour differs: native -> falls into otherwise(); a naive UDF
    gets None and crashes on `None >= 150` (TypeError).
  - Rule of thumb: native first, UDF only when no native function can do it.

An "Arrow optimization" warning may appear: Spark fell back to a normal
Python UDF. It is harmless here.

# Tested with pyspark 4.2.0, Java 17
"""

import contextlib
import io

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("HandbookUDF").master("local[*]").getOrCreate()


def plan_text(dataframe):
    """Capture explain() output as a string so we can assert on it."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        dataframe.explain()
    return buffer.getvalue()


orders = [
    (101, 120),
    (102, 80),
    (103, 200),
    (104, 150),
]

df = spark.createDataFrame(orders, ["order_id", "amount"])

# --- Method 1: native ---------------------------------------------------------
native_df = df.withColumn(
    "category", F.when(F.col("amount") >= 150, "high").otherwise("low")
)

print("\n1. Native Spark functions:")
native_df.show()
print("\n2. Native execution plan:")
native_df.explain()


# --- Method 2: Python UDF -----------------------------------------------------
def categorize_amount(amount):
    if amount >= 150:
        return "high"
    return "low"


categorize_udf = F.udf(categorize_amount, StringType())
udf_df = df.withColumn("category", categorize_udf(F.col("amount")))

print("\n3. Python UDF:")
udf_df.show()
print("\n4. UDF execution plan:")
udf_df.explain()

# --- Compare ------------------------------------------------------------------
expected = {101: "low", 102: "low", 103: "high", 104: "high"}


def to_dict(dataframe):
    return {row["order_id"]: row["category"] for row in dataframe.collect()}


assert to_dict(native_df) == expected
assert to_dict(udf_df) == expected
assert to_dict(native_df) == to_dict(udf_df)

# Plan evidence
assert "EvalPython" in plan_text(udf_df)  # BatchEvalPython or ArrowEvalPython
assert "EvalPython" not in plan_text(native_df)

print("\nNative functions and UDF checks passed!")

# --- NULL behaviour -------------------------------------------------------------
null_df = spark.createDataFrame(
    [(201, 100), (202, None)],
    "order_id int, amount int",
)

native_null_df = null_df.withColumn(
    "category", F.when(F.col("amount") >= 150, "high").otherwise("low")
)
udf_null_df = null_df.withColumn("category", categorize_udf(F.col("amount")))

print("\n5. NULL with native function:")
native_null_df.show()

# NULL >= 150 is NULL (not true) -> otherwise("low")
assert to_dict(native_null_df) == {201: "low", 202: "low"}

# The UDF receives None -> `None >= 150` raises TypeError inside the Python
# worker. Spark reports it as an exception when an action runs.
udf_failed = False
try:
    udf_null_df.collect()
except Exception as error:
    udf_failed = True
    print("\n6. UDF with NULL failed as expected:", type(error).__name__)

assert udf_failed


# Fix: handle None explicitly inside the UDF
def categorize_amount_safe(amount):
    if amount is None:
        return "unknown"
    return "high" if amount >= 150 else "low"


safe_udf = F.udf(categorize_amount_safe, StringType())
safe_df = null_df.withColumn("category", safe_udf(F.col("amount")))

assert to_dict(safe_df) == {201: "low", 202: "unknown"}

print("\nNULL behaviour checks passed!")

spark.stop()
