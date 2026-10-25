# Tested with pyspark 4.2.0, Java 17
# ============================================================
# PySpark 05: row_number vs rank vs dense_rank (ties)
#
# Problem:
#   Two orders of the same customer can have the same amount. See how
#   row_number, rank and dense_rank number them, and why only row_number
#   is safe for "keep one row per customer".
#
# Pattern:
#   tie_window    = Window.partitionBy("customer").orderBy(F.col("amount").desc())
#   unique_window = tie_window + F.col("order_id").asc()   (tie-breaker)
#   F.row_number().over(unique_window)   1, 2, 3, 4   (always unique)
#   F.rank().over(tie_window)            1, 1, 3, 4   (ties share a rank, gap after)
#   F.dense_rank().over(tie_window)      1, 1, 2, 3   (ties share a rank, no gap)
#
# Verified (assert):
#   - all three numberings for all 7 orders (Ali and Ayse)
#   - filter row_num == 1 gives one order per customer: 101 and 105
#   - filter rank_num == 1 gives every tied order: 101, 102, 105, 106
#     (4 rows, so using rank to deduplicate would leave duplicates)
#
# NOT covered:
#   - row_number on tie_window (without a tie-breaker): which tied row gets
#     1 is not guaranteed and can change between runs
#   - ties of more than two rows, null sort keys, big data
#
# Notes:
#   * rank leaves a gap after a tie (1, 1, 3); dense_rank does not (1, 1, 2).
#   * To keep one row per key (deduplicate), use row_number with a unique
#     tie-breaker such as order_id or a timestamp.
#   * To show every tied winner, use rank or dense_rank.
# ============================================================

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("HandbookRanking").master("local[*]").getOrCreate()

orders = [
    (101, "Ali", 200),
    (102, "Ali", 200),
    (103, "Ali", 120),
    (104, "Ali", 90),
    (105, "Ayse", 170),
    (106, "Ayse", 170),
    (107, "Ayse", 80),
]

df = spark.createDataFrame(orders, ["order_id", "customer", "amount"])

# Ties share a rank
tie_window = Window.partitionBy("customer").orderBy(F.col("amount").desc())

# order_id breaks ties, so every row gets a unique number
unique_window = Window.partitionBy("customer").orderBy(
    F.col("amount").desc(), F.col("order_id").asc()
)

ranked_df = (
    df.withColumn("row_num", F.row_number().over(unique_window))
    .withColumn("rank_num", F.rank().over(tie_window))
    .withColumn("dense_rank_num", F.dense_rank().over(tie_window))
)

print("\nRanking comparison:")
ranked_df.orderBy("customer", "row_num").show()

all_ranks = {
    row["order_id"]: (row["row_num"], row["rank_num"], row["dense_rank_num"])
    for row in ranked_df.collect()
}

assert all_ranks == {
    101: (1, 1, 1),  # Ali
    102: (2, 1, 1),
    103: (3, 3, 2),
    104: (4, 4, 3),
    105: (1, 1, 1),  # Ayse
    106: (2, 1, 1),
    107: (3, 3, 2),
}

assert ranked_df.count() == 7

# --- Why it matters: "keep one row per customer" ---
one_per_customer = ranked_df.filter(F.col("row_num") == 1)
ties_included = ranked_df.filter(F.col("rank_num") == 1)

assert {row["order_id"] for row in one_per_customer.collect()} == {101, 105}
assert one_per_customer.count() == 2  # one row per customer

assert {row["order_id"] for row in ties_included.collect()} == {101, 102, 105, 106}
assert ties_included.count() == 4  # tied orders all stay: not a deduplication

print("\nAll PySpark ranking checks passed!")

spark.stop()
