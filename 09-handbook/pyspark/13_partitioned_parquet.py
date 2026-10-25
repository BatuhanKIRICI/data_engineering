"""
13 - Partitioned Parquet (partitionBy)

What this file shows:
  - write.partitionBy("order_date") creates one folder per value:
        orders/order_date=2026-10-23/part-....parquet
  - The partition value lives in the FOLDER NAME, not inside the files.
    So the plan's ReadSchema does not contain order_date.
  - Filtering on the partition column -> PartitionFilters in the plan
    (Spark skips whole folders = partition pruning).
  - Spark infers the partition column type when reading back. A value like
    "2026-10-24" may come back as a date, not a string. We print the type
    instead of asserting it.
  - Files per folder: repartition("order_date") before writing gives one
    file per folder.
  - Only partition by LOW-cardinality columns (date, country). Something like
    order_id would create one folder per row.

# Tested with pyspark 4.2.0, Java 17
"""

import contextlib
import io
import re
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("HandbookPartitionedParquet")
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
    (101, "2026-10-23", 120),
    (102, "2026-10-23", 80),
    (103, "2026-10-24", 200),
    (104, "2026-10-24", 150),
]

df = spark.createDataFrame(orders, ["order_id", "order_date", "amount"])

with TemporaryDirectory() as temp_dir:
    output_path = Path(temp_dir) / "orders"

    # Organize Parquet files by order_date
    df.write.mode("overwrite").partitionBy("order_date").parquet(str(output_path))

    folders = sorted(p.name for p in output_path.iterdir() if p.is_dir())
    print("\n1. Partitioned folders:")
    for name in folders:
        print(name)

    assert folders == ["order_date=2026-10-23", "order_date=2026-10-24"]

    # Read back
    parquet_df = spark.read.parquet(str(output_path))

    print("\n2. Read Parquet:")
    parquet_df.orderBy("order_id").show()
    print("Column types (order_date type is inferred):", parquet_df.dtypes)

    # Filter on the partition column
    filtered_df = parquet_df.filter(F.col("order_date") == "2026-10-24")

    print("\n3. Filtered rows:")
    filtered_df.show()

    print("\n4. Execution plan:")
    filtered_df.explain()

    plan = plan_text(filtered_df)
    assert "PartitionFilters" in plan
    # The scan node prints PartitionFilters and ReadSchema on ONE line, so we
    # extract only the ReadSchema struct instead of checking the whole line.
    read_schemas = re.findall(r"ReadSchema: struct<[^>]*>", plan)
    assert read_schemas
    # Partition value is not stored inside the files:
    assert all("order_date" not in schema for schema in read_schemas)

    assert parquet_df.count() == 4
    assert filtered_df.count() == 2
    assert filtered_df.agg(F.sum("amount")).first()[0] == 350

    # --- Files per folder ---------------------------------------------------
    # repartition("order_date") puts all rows of one date into one partition,
    # so each folder gets a single data file.
    compact_path = Path(temp_dir) / "orders_compact"
    (
        df.repartition("order_date")
        .write.mode("overwrite")
        .partitionBy("order_date")
        .parquet(str(compact_path))
    )

    print("\n5. Parquet files per folder after repartition('order_date'):")
    for folder in sorted(p for p in compact_path.iterdir() if p.is_dir()):
        files = list(folder.glob("*.parquet"))
        print(folder.name, len(files))
        assert len(files) == 1

    print("\nPartitioned Parquet checks passed!")

spark.stop()
