# Tested with pyspark 4.2.0, delta-spark 4.4.1, Java 17
"""
17 - Delta Lake MERGE (upsert)

What this file shows:
  - A Delta table = Parquet files + a _delta_log (transaction log).
  - MERGE: matching order_id -> UPDATE, new order_id -> INSERT.
    (The left_anti load in 15 could only insert.)
  - Idempotence: running the same MERGE again leaves the data unchanged.
  - Every write is a new table VERSION; history() lists them and
    versionAsOf reads an old one (time travel).
  - Atomicity: if the source has duplicate keys, MERGE fails and the table
    stays exactly as it was. Deduplicate the source first (see 15).

The first run downloads the Delta JAR from Maven (needs internet) and prints a
long Ivy log. That is normal.
"""

import tempfile

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.appName("HandbookDeltaMerge")
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

existing_df = spark.createDataFrame(
    [(101, "Ali", 120), (102, "Ayse", 80), (103, "Ali", 200)], columns
)
incoming_df = spark.createDataFrame([(103, "Ali", 250), (104, "Mehmet", 150)], columns)


def table_as_dict(path, **read_options):
    reader = spark.read.format("delta")
    for key, value in read_options.items():
        reader = reader.option(key, value)
    return {
        row["order_id"]: (row["customer"], row["amount"])
        for row in reader.load(path).collect()
    }


def run_merge(path, source_df):
    (
        DeltaTable.forPath(spark, path)
        .alias("target")
        .merge(source_df.alias("source"), "target.order_id = source.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


with tempfile.TemporaryDirectory() as delta_path:
    # --- Version 0: initial table ---------------------------------------------
    existing_df.write.format("delta").mode("overwrite").save(delta_path)

    print("\n1. Before MERGE:")
    spark.read.format("delta").load(delta_path).orderBy("order_id").show()

    before = {101: ("Ali", 120), 102: ("Ayse", 80), 103: ("Ali", 200)}
    assert table_as_dict(delta_path) == before

    # --- Version 1: MERGE -----------------------------------------------------
    run_merge(delta_path, incoming_df)

    print("\n2. After MERGE:")
    spark.read.format("delta").load(delta_path).orderBy("order_id").show()

    after = {
        101: ("Ali", 120),  # unchanged
        102: ("Ayse", 80),  # unchanged
        103: ("Ali", 250),  # UPDATED 200 -> 250
        104: ("Mehmet", 150),  # INSERTED
    }
    assert table_as_dict(delta_path) == after

    # --- Idempotence: same batch again ---------------------------------------
    run_merge(delta_path, incoming_df)
    assert table_as_dict(delta_path) == after
    print("\n3. Second run of the same MERGE: data unchanged (idempotent)")

    # --- History and time travel ---------------------------------------------
    history_df = DeltaTable.forPath(spark, delta_path).history()

    print("\n4. Table history:")
    history_df.select("version", "operation").orderBy("version").show()

    operations = {r["version"]: r["operation"] for r in history_df.collect()}
    assert operations[0] == "WRITE"
    assert operations[1] == "MERGE"

    # Version 0 is still readable, even though the table has changed since
    assert table_as_dict(delta_path, versionAsOf=0) == before
    print("5. Time travel: versionAsOf=0 still shows the original amount of 103")

    # --- Atomicity: duplicate keys in the source -----------------------------
    duplicate_source_df = spark.createDataFrame(
        [(103, "Ali", 300), (103, "Ali", 310)], columns
    )

    merge_failed = False
    try:
        run_merge(delta_path, duplicate_source_df)
    except Exception as error:
        merge_failed = True
        print("\n6. MERGE with duplicate source keys failed:", type(error).__name__)

    assert merge_failed
    assert table_as_dict(delta_path) == after  # table untouched

print("\nDelta MERGE checks passed!")

spark.stop()
