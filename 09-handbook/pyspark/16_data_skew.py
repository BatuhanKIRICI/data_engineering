# Tested with pyspark 4.2.0, Java 17
"""
16 - Data skew

What this file shows:
  - Skew: one key (Ali) owns most rows, so one partition does most of the work
    and the slowest task delays the whole stage.
  - repartition(4, "customer") hashes the key: all rows with the same
    customer land in the SAME partition. So Ali's 900 rows cannot be split.
  - Salting: add a random suffix to the hot key, aggregate in two steps
    (customer+salt first, then customer). Same result, smaller tasks.

This is a distribution demo, NOT a performance test: 1000 rows in local mode
are never slow. Which partition ids are filled depends on the hash, so only
facts that are guaranteed are asserted (total rows, largest partition >= 900).

AQE (spark.sql.adaptive.skewJoin.enabled) can split skewed partitions in
JOINS automatically, but it needs far larger partitions than this demo, so it
will not trigger here.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("HandbookDataSkew").master("local[*]").getOrCreate()
)

# Create an intentionally skewed dataset
orders = (
    [(i, "Ali", 100) for i in range(1, 901)]
    + [(i, "Ayse", 100) for i in range(901, 951)]
    + [(i, "Mehmet", 100) for i in range(951, 1001)]
)

df = spark.createDataFrame(orders, ["order_id", "customer", "amount"])

print("\n1. Customer distribution:")
df.groupBy("customer").count().orderBy(F.col("count").desc()).show()

# Distribute rows by customer
partitioned_df = df.repartition(4, "customer")

print("\n2. Partition count:")
print(partitioned_df.rdd.getNumPartitions())

# Count rows in each physical partition
partition_sizes = (
    partitioned_df.withColumn("partition_id", F.spark_partition_id())
    .groupBy("partition_id")
    .count()
    .orderBy("partition_id")
)

print("\n3. Rows per partition:")
partition_sizes.show()

print("\n4. Repartition execution plan (expect Exchange hashpartitioning):")
partitioned_df.explain()

# --- Validate ------------------------------------------------------------------
assert df.count() == 1000
assert partitioned_df.rdd.getNumPartitions() == 4

customer_counts = {
    row["customer"]: row["count"] for row in df.groupBy("customer").count().collect()
}
assert customer_counts == {"Ali": 900, "Ayse": 50, "Mehmet": 50}

sizes = [row["count"] for row in partition_sizes.collect()]
assert sum(sizes) == 1000  # no row lost
assert max(sizes) >= 900  # same key -> same partition, so Ali cannot be split
assert len(sizes) <= 4  # some partitions may be empty (not listed)

print("\nData skew checks passed!")

# --- Salting: split the hot key, aggregate twice -------------------------------
SALTS = 4

salted_df = df.withColumn("salt", F.floor(F.rand(seed=42) * SALTS).cast("int"))

# Step 1: partial totals per (customer, salt) -> Ali is split into up to 4 groups
partial_df = salted_df.groupBy("customer", "salt").agg(
    F.sum("amount").alias("partial_total"),
    F.count("*").alias("rows"),
)

print("\n5. Step 1 - partial totals (Ali is now spread over several groups):")
partial_df.orderBy("customer", "salt").show()

# Step 2: combine the partials per customer
salted_totals_df = partial_df.groupBy("customer").agg(
    F.sum("partial_total").alias("total_amount")
)

print("\n6. Step 2 - final totals:")
salted_totals_df.orderBy("customer").show()

# Same answer as the plain aggregation
plain = {
    row["customer"]: row["total_amount"]
    for row in df.groupBy("customer")
    .agg(F.sum("amount").alias("total_amount"))
    .collect()
}
salted = {row["customer"]: row["total_amount"] for row in salted_totals_df.collect()}

assert plain == {"Ali": 90000, "Ayse": 5000, "Mehmet": 5000}
assert salted == plain

# The hot key is no longer one group of 900 rows
ali_groups = [
    r["rows"] for r in partial_df.filter(F.col("customer") == "Ali").collect()
]
assert sum(ali_groups) == 900
assert max(ali_groups) < 900

print("\nAQE skew join enabled:", spark.conf.get("spark.sql.adaptive.skewJoin.enabled"))
print("\nSalting checks passed!")

spark.stop()
