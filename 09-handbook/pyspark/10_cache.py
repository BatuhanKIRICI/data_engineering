"""
10 - cache() and persist()

What this file shows:
  - cache() only MARKS a DataFrame; the first action fills the cache.
  - Evidence in the plan: InMemoryTableScan / InMemoryRelation appear
    while cached, and disappear after unpersist().
  - persist(level) lets you choose the storage level explicitly.
  - Cache is a snapshot of the data at materialization time. If the source
    changes afterwards, a cached DataFrame will NOT see it. Always unpersist()
    when you are done.

# Tested with pyspark 4.2.0, Java 17
"""

import contextlib
import io

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("HandbookCache").master("local[*]").getOrCreate()


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

filtered_df = df.filter(F.col("amount") > 100)

# Before cache: plan has no in-memory scan.
# NOTE: we check a FRESH DataFrame (df.filter(...)) on purpose. A DataFrame
# object remembers the plan it computed the first time, so calling explain()
# on filtered_df here could keep showing the old plan after cache().
assert "InMemoryTableScan" not in plan_text(df.filter(F.col("amount") > 100))

cached_df = filtered_df.cache()

print("\n1. Cache registered (nothing computed yet):")
print("Is cached:", cached_df.is_cached)

print("\n2. First action materializes the cache:")
cached_df.show()

print("\n3. Second action reuses it:")
print("Row count:", cached_df.count())

print("\n4. Cached execution plan:")
cached_df.explain()

assert cached_df.is_cached
assert cached_df.count() == 4
assert "InMemoryTableScan" in plan_text(cached_df)

# --- unpersist ---------------------------------------------------------------
cached_df.unpersist()

print("\n5. After unpersist:")
print("Is cached:", cached_df.is_cached)

assert not cached_df.is_cached
assert "InMemoryTableScan" not in plan_text(df.filter(F.col("amount") > 100))

# --- persist with explicit storage level ------------------------------------
persisted_df = filtered_df.persist(StorageLevel.MEMORY_AND_DISK)

print("\n6. Persist storage level:")
print(persisted_df.storageLevel)

assert persisted_df.count() == 4
assert persisted_df.storageLevel.useMemory
assert persisted_df.storageLevel.useDisk

persisted_df.unpersist()
assert not persisted_df.is_cached

print("\nCache and persist checks passed!")

spark.stop()
