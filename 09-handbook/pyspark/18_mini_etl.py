# Tested with pyspark 4.2.0, delta-spark 4.4.1, Java 17
"""
18 - Mini PySpark ETL: clean -> validate -> split -> MERGE -> reconcile

Flow:
  raw orders
    -> clean (trim/upper customer, try_cast amount)
    -> rejected  : amount missing or not a number (with a reason)
    -> valid     : everything else
         -> drop exact duplicates
         -> Delta MERGE into the target table (update existing, insert new)
    -> reconciliation:  raw == rejected + accepted + duplicates
                        (rows AND money)

Design choices:
  - Only EXACT duplicate rows are dropped. Same order_id with a different
    amount raises an error: without a timestamp we don't guess which is newer.
  - Rows are never silently lost; rejected rows keep a reason.
  - Running the pipeline twice must leave the target unchanged (idempotent).
"""

import tempfile

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

builder = (
    SparkSession.builder.appName("HandbookMiniETL")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

columns = ["order_id", "customer", "amount"]

raw_orders = [
    (101, " Ali ", "120"),
    (102, "AYSE", "80"),
    (103, "Mehmet", None),  # missing amount   -> rejected
    (104, "Ali", "200"),
    (104, "Ali", "200"),  # exact duplicate  -> dropped
    (105, "Ayse", "invalid"),  # not a number     -> rejected
    (106, " Mehmet ", "150"),
]

# Target already contains an OLD version of 101 and an unrelated order 199
existing_target = [(101, "ALI", 100), (199, "OLD", 50)]


def run_pipeline(raw_df, target_path):
    """Run one batch. Returns the reconciliation numbers and the rejected rows."""
    # 1. Clean
    cleaned_df = raw_df.select(
        "order_id",
        F.upper(F.trim(F.col("customer"))).alias("customer"),
        F.col("amount").alias("raw_amount"),
        F.expr("try_cast(amount as int)").alias("amount"),
    )

    # 2. Split valid / rejected (with a reason)
    rejected_df = cleaned_df.filter(F.col("amount").isNull()).withColumn(
        "reason",
        F.when(F.col("raw_amount").isNull(), "missing_amount").otherwise(
            "invalid_amount"
        ),
    )
    valid_df = cleaned_df.filter(F.col("amount").isNotNull()).select(*columns)

    # 3. Drop exact duplicates; a conflicting duplicate key is an error
    deduplicated_df = valid_df.dropDuplicates(columns)
    if deduplicated_df.count() != deduplicated_df.select("order_id").distinct().count():
        raise ValueError("Same order_id with different values; need a timestamp")

    # 4. Upsert into the Delta target
    (
        DeltaTable.forPath(spark, target_path)
        .alias("target")
        .merge(deduplicated_df.alias("source"), "target.order_id = source.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    # 5. Reconciliation numbers (rows and money)
    def total(df):
        return df.agg(F.coalesce(F.sum("amount"), F.lit(0))).first()[0]

    stats = {
        "raw": raw_df.count(),
        "rejected": rejected_df.count(),
        "accepted": deduplicated_df.count(),
        "duplicates": valid_df.count() - deduplicated_df.count(),
        "accepted_amount": total(deduplicated_df),
    }
    return stats, rejected_df


def target_as_dict(path):
    rows = spark.read.format("delta").load(path).collect()
    return {r["order_id"]: (r["customer"], r["amount"]) for r in rows}


raw_df = spark.createDataFrame(raw_orders, columns)

print("\n1. Raw data:")
raw_df.show()

with tempfile.TemporaryDirectory() as target_path:
    spark.createDataFrame(existing_target, columns).write.format("delta").save(
        target_path
    )

    stats, rejected_df = run_pipeline(raw_df, target_path)

    print("\n2. Rejected orders:")
    rejected_df.select("order_id", "customer", "raw_amount", "reason").orderBy(
        "order_id"
    ).show()

    print("\n3. Final Delta target:")
    spark.read.format("delta").load(target_path).orderBy("order_id").show()

    print("\n4. Reconciliation:", stats)

    # --- Rejected rows --------------------------------------------------------
    reasons = {r["order_id"]: r["reason"] for r in rejected_df.collect()}
    assert reasons == {103: "missing_amount", 105: "invalid_amount"}

    # --- Target after MERGE ---------------------------------------------------
    expected_target = {
        101: ("ALI", 120),  # UPDATED 100 -> 120
        102: ("AYSE", 80),  # inserted
        104: ("ALI", 200),  # inserted once (duplicate dropped)
        106: ("MEHMET", 150),  # inserted
        199: ("OLD", 50),  # untouched
    }
    assert target_as_dict(target_path) == expected_target

    # --- Reconciliation: nothing silently lost -------------------------------
    assert stats["raw"] == 7
    assert stats["rejected"] == 2
    assert stats["accepted"] == 4
    assert stats["duplicates"] == 1
    assert stats["raw"] == stats["rejected"] + stats["accepted"] + stats["duplicates"]
    assert stats["accepted_amount"] == 120 + 80 + 200 + 150  # 550

    # --- Idempotence: same batch again leaves the target unchanged -----------
    second_stats, _ = run_pipeline(raw_df, target_path)
    assert target_as_dict(target_path) == expected_target
    assert second_stats == stats
    print("\n5. Second run: target unchanged (idempotent)")

    # --- Conflicting duplicate must fail loudly -------------------------------
    conflict_df = spark.createDataFrame(
        [(300, "Ali", "10"), (300, "Ali", "20")], columns
    )
    conflict_failed = False
    try:
        run_pipeline(conflict_df, target_path)
    except ValueError as error:
        conflict_failed = True
        print("\n6. Conflicting duplicate rejected:", error)

    assert conflict_failed
    assert target_as_dict(target_path) == expected_target  # target untouched

print("\nMini ETL checks passed!")

spark.stop()
