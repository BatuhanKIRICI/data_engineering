# Tested with pyspark 4.2.0, Java 17
"""
15 - Deduplication and insert-only incremental load

What this file shows:
  - dropDuplicates(["order_id"]) removes repeated keys inside a batch.
    If the SAME key arrives with DIFFERENT values, it does not guarantee
    which row survives. Then use row_number() over updated_at (section 2).
  - left_anti join keeps only incoming rows whose key is NOT in the target.
  - unionByName appends them, matching columns by name.
  - Idempotence: running the same batch again must add 0 rows.
  - This is insert-only. Changed amounts for existing orders are NOT updated.
    Updates (upsert) come later with Delta Lake MERGE.
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("HandbookIncrementalETL")
    .master("local[*]")
    .getOrCreate()
)

columns = ["order_id", "customer", "amount"]

existing_df = spark.createDataFrame(
    [(101, "Ali", 120), (102, "Ayse", 80), (103, "Ali", 200)], columns
)
incoming_df = spark.createDataFrame(
    [(103, "Ali", 200), (104, "Mehmet", 150), (104, "Mehmet", 150), (105, "Ayse", 170)],
    columns,
)


def incremental_load(target_df, batch_df):
    """Return (new_orders_df, updated_target_df) for an insert-only load."""
    deduplicated_df = batch_df.dropDuplicates(["order_id"])
    new_orders_df = deduplicated_df.join(
        target_df.select("order_id"), on="order_id", how="left_anti"
    )
    # left_anti join moved order_id first, so select columns explicitly
    new_orders_df = new_orders_df.select(*target_df.columns)
    return new_orders_df, target_df.unionByName(new_orders_df)


print("\n1. Incoming batch:")
incoming_df.show()

deduplicated_df = incoming_df.dropDuplicates(["order_id"])
print("\n2. After deduplication:")
deduplicated_df.orderBy("order_id").show()

new_orders_df, updated_df = incremental_load(existing_df, incoming_df)

print("\n3. New orders only:")
new_orders_df.orderBy("order_id").show()

print("\n4. Updated target:")
updated_df.orderBy("order_id").show()

# --- Validate the first run ----------------------------------------------------
assert incoming_df.count() == 4
assert deduplicated_df.count() == 3
assert {row["order_id"] for row in new_orders_df.collect()} == {104, 105}
assert updated_df.count() == 5
assert updated_df.select("order_id").distinct().count() == 5  # no duplicate keys

expected = {
    101: ("Ali", 120),
    102: ("Ayse", 80),
    103: ("Ali", 200),
    104: ("Mehmet", 150),
    105: ("Ayse", 170),
}
assert {
    r["order_id"]: (r["customer"], r["amount"]) for r in updated_df.collect()
} == expected

# --- Idempotence: process the SAME batch again --------------------------------
second_new_df, second_target_df = incremental_load(updated_df, incoming_df)

print("\n5. Second run (same batch):")
print("New records:", second_new_df.count())

assert second_new_df.count() == 0  # idempotent
assert second_target_df.count() == 5

print("\nIncremental load checks passed!")

# --- Same key, different values: keep the latest by updated_at ----------------
versions_df = spark.createDataFrame(
    [
        (200, "Ali", 100, "2026-10-23"),
        (200, "Ali", 110, "2026-10-24"),
        (201, "Ayse", 80, "2026-10-23"),
    ],
    ["order_id", "customer", "amount", "updated_at"],
)

latest_window = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())

latest_df = (
    versions_df.withColumn("rn", F.row_number().over(latest_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

print("\n6. Latest version per order_id:")
latest_df.orderBy("order_id").show()

assert latest_df.count() == 2
assert {r["order_id"]: r["amount"] for r in latest_df.collect()} == {200: 110, 201: 80}

print("\nLatest-record checks passed!")

spark.stop()
