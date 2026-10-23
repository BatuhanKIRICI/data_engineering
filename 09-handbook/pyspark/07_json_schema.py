# ============================================================
# PySpark 07: JSON Lines with an explicit schema (StructType) and bad records
#
# Problem:
#   Read a JSON Lines file with a schema we define ourselves instead of
#   letting Spark guess, and see what happens when a record does not match
#   the schema.
#
# Pattern:
#   schema = StructType([StructField("name", IntegerType() | StringType(), nullable=...), ...])
#   spark.read.schema(schema).json(path)
#   .option("mode", "PERMISSIVE")   bad value becomes NULL, the job keeps going
#   .option("mode", "FAILFAST")     the job stops on the first bad record
#   StructField("_corrupt_record", StringType())   keeps the raw bad line
#
# Verified (assert):
#   - 4 rows, 3 columns, types int / string / int
#   - exact row contents and total amount 550 (hand-calculated)
#   - PERMISSIVE: an "abc" amount does not fail the job; amount becomes NULL
#     and the raw bad line is kept in _corrupt_record
#   - FAILFAST: the same bad record raises an error
#
# NOT covered:
#   - what happens to the OTHER fields of a bad record (order_id / customer);
#     this is printed, not asserted, because it depends on the Spark version
#   - DROPMALFORMED mode, multiLine JSON, nested JSON, DecimalType for money
#   - sending bad records to a quarantine table
#
# Notes:
#   * The input must be JSON Lines: one object per line. Pretty-printed JSON
#     (made by an editor's "Format Document") breaks the read. Use
#     .option("multiLine", True) for that kind of file.
#   * nullable=False in the schema is NOT enforced when reading files: Spark
#     shows the columns as nullable = true. Check nulls yourself.
#   * An explicit schema does not validate values. PERMISSIVE turns bad values
#     into NULL silently, so a pipeline can stay green with wrong data.
#   * collect() pulls all rows to the driver: fine here, not for big data.
# ============================================================

import tempfile
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

spark = (
    SparkSession.builder.appName("HandbookJSONSchema").master("local[*]").getOrCreate()
)

base_dir = Path(__file__).resolve().parent
json_path = base_dir / "data" / "orders.jsonl"

# --- 1. Read with an explicit schema ---
order_schema = StructType(
    [
        StructField("order_id", IntegerType(), nullable=False),
        StructField("customer", StringType(), nullable=True),
        StructField("amount", IntegerType(), nullable=True),
    ]
)

df = spark.read.schema(order_schema).json(str(json_path))

print("\nJSON data:")
df.show()

print("\nJSON schema:")
df.printSchema()

assert df.count() == 4
assert df.columns == ["order_id", "customer", "amount"]
assert dict(df.dtypes) == {
    "order_id": "int",
    "customer": "string",
    "amount": "int",
}

rows = sorted((row["order_id"], row["customer"], row["amount"]) for row in df.collect())

assert rows == [
    (101, "Ali", 120),
    (102, "Ayse", 80),
    (103, "Ali", 200),
    (104, "Mehmet", 150),
]
assert df.agg({"amount": "sum"}).first()[0] == 550

print("\nJSON schema and content checks passed!")

# --- 2. A bad record: amount is "abc", not an integer ---
bad_path = Path(tempfile.mkdtemp()) / "bad.jsonl"
bad_path.write_text(
    '{"order_id":105,"customer":"Ali","amount":"abc"}\n', encoding="utf-8"
)

# PERMISSIVE: keep going, keep the raw bad line in _corrupt_record
bad_schema = StructType(
    list(order_schema.fields)
    + [StructField("_corrupt_record", StringType(), nullable=True)]
)

bad_df = (
    spark.read.schema(bad_schema)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .json(str(bad_path))
)

print("\nBad JSON with PERMISSIVE:")
bad_df.show(truncate=False)

bad_rows = bad_df.collect()

assert len(bad_rows) == 1
assert bad_rows[0]["amount"] is None  # "abc" did not become an integer
assert bad_rows[0]["_corrupt_record"] is not None  # raw line is kept

# The other fields of the bad record depend on the Spark version: print only
print("order_id of the bad record:", bad_rows[0]["order_id"])
print("customer of the bad record:", bad_rows[0]["customer"])

print("\nPERMISSIVE checks passed!")

# --- 3. FAILFAST: stop on the first bad record ---
strict_df = (
    spark.read.schema(order_schema).option("mode", "FAILFAST").json(str(bad_path))
)

try:
    strict_df.collect()  # collect() forces every column to be parsed
except Exception:
    print("\nFAILFAST caught the bad record.")
else:
    raise AssertionError("FAILFAST should have failed on the bad record")

print("\nFAILFAST checks passed!")

print("\nAll PySpark JSON schema checks passed!")

spark.stop()
