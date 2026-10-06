"""
Pattern 01: Top customers by total spend (SQL -> pandas) with reconciliation.

Problem   : Find the top 10 customers by total expenditure in Chinook.
Approach 1: customer -> invoice                -> sum(invoice.total)
Approach 2: customer -> invoice -> invoice_line -> sum(unit_price * quantity)
Check     : both approaches must agree for ALL customers (reconciliation).
"""

import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# --- Connection ---------------------------------------------------------------
load_dotenv()  # .env -> environment variables (must run before os.getenv)

url = URL.create(
    "postgresql+psycopg2",
    username=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    database="chinook",
)
engine = create_engine(url)

# --- Load only the columns we need -------------------------------------------
# Note: read_sql returned float64 for the numeric columns here. If you ever see
# dtype "object" (Decimal), add .astype(float) before doing arithmetic.
customer_df = pd.read_sql(
    "select customer_id, first_name, last_name from customer", engine
)
invoice_df = pd.read_sql("select invoice_id, customer_id, total from invoice", engine)
invoice_line_df = pd.read_sql(
    "select invoice_id, unit_price, quantity from invoice_line", engine
)

# --- Approach 1: invoice.total -------------------------------------------------
customer_invoice_1 = customer_df.merge(invoice_df, on="customer_id", how="inner")
assert len(customer_invoice_1) == len(invoice_df), "Join changed the number of invoices"

spend_1 = (
    customer_invoice_1.groupby(
        ["customer_id", "first_name", "last_name"], as_index=False
    )["total"]
    .sum()
    .rename(columns={"total": "total_expenditure"})
)

# --- Approach 2: invoice_line (unit_price * quantity) ---------------------------
# Compute the money per line BEFORE aggregating. Do not carry invoice.total into
# this join: it would be repeated once per line and summed too many times.
invoice_line_df["line_total"] = (
    invoice_line_df["unit_price"] * invoice_line_df["quantity"]
)

customer_invoice_2 = customer_df.merge(
    invoice_df[["invoice_id", "customer_id"]], on="customer_id", how="inner"
)
customer_lines = customer_invoice_2.merge(
    invoice_line_df[["invoice_id", "line_total"]], on="invoice_id", how="inner"
)
assert len(customer_lines) == len(
    invoice_line_df
), "Join changed the number of invoice lines"

spend_2 = (
    customer_lines.groupby(["customer_id", "first_name", "last_name"], as_index=False)[
        "line_total"
    ]
    .sum()
    .rename(columns={"line_total": "total_expenditure"})
)

# --- Reconciliation: all customers, not only the top 10 --------------------------
check = spend_1[["customer_id", "total_expenditure"]].merge(
    spend_2[["customer_id", "total_expenditure"]],
    on="customer_id",
    how="outer",
    suffixes=("_invoice", "_line"),
)
diff = (check["total_expenditure_invoice"] - check["total_expenditure_line"]).abs()

assert check.notna().all().all(), "Customer exists in only one approach"
assert (diff < 0.005).all(), "The two approaches do not match"
print("Reconciliation passed. Maximum difference:", diff.max())

# --- Result ------------------------------------------------------------------
# customer_id as the second key makes ties deterministic (same order every run).
top_10 = spend_1.sort_values(
    ["total_expenditure", "customer_id"], ascending=[False, True]
).head(10)
print(top_10.to_string(index=False))
