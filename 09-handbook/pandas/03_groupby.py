# ============================================================
# Pandas 03: groupby, aggregation, sorting, top N
#
# Problem:
#   Total spend, order count and average order value per customer,
#   then the top 2 customers by total spend.
#
# Pattern:
#   df.groupby("customer")["amount"].sum()      -> Series (customer = index)
#   .reset_index()                              -> back to a DataFrame
#   .groupby(...).agg(name=("col", "func"))     -> several metrics at once
#   .sort_values(by=[...], ascending=[...])     -> order by (with tie-break)
#   .head(n)                                    -> limit n
#
# Verified (assert):
#   - 3 customers, row count preserved (5 orders)
#   - sum of group totals == source total == 580 (hand-calculated)
#   - top 2 = Ali, Ayse (both 250); tie broken by customer name
#
# NOT covered:
#   - groupby drops rows whose key is NaN (dropna=True by default),
#     so a missing customer would silently disappear from the totals
#   - head(2) cuts ties at the boundary (nlargest(keep="all") keeps them)
#   - large data
#
# Notes:
#   * count = SQL count(col) (ignores NaN); size = count(*)
#   * sort_values needs a second key for a repeatable order on ties
# ============================================================

import pandas as pd

orders = [
    {"customer": "Ali", "amount": 100},
    {"customer": "Ayse", "amount": 200},
    {"customer": "Ali", "amount": 150},
    {"customer": "Mehmet", "amount": 80},
    {"customer": "Ayse", "amount": 50},
]

df = pd.DataFrame(orders)

print("Original orders:")
print(df)

# --- 1. One metric per group: Series, then back to DataFrame ---
customer_totals = df.groupby("customer")["amount"].sum()

print("\nCustomer totals (Series, customer is the index):")
print(customer_totals)

customer_totals_df = customer_totals.reset_index(name="total_amount")

print("\nCustomer totals as DataFrame:")
print(customer_totals_df)

# --- 2. Several metrics at once ---
customer_summary = (
    df.groupby("customer")
    .agg(
        total_amount=("amount", "sum"),
        order_count=("amount", "count"),
        average_amount=("amount", "mean"),
    )
    .reset_index()
)

print("\nCustomer summary:")
print(customer_summary)

# --- 3. Sort (tie-break on customer) and take top N ---
top_customers = customer_summary.sort_values(
    by=["total_amount", "customer"], ascending=[False, True]
).reset_index(drop=True)

print("\nCustomers sorted:")
print(top_customers)

top_2_customers = top_customers.head(2)

print("\nTop 2 customers:")
print(top_2_customers)

# --- Checks ---
assert len(customer_summary) == 3

# Compared with the source, and with a hand-calculated value
assert customer_summary["total_amount"].sum() == df["amount"].sum()
assert customer_summary["total_amount"].sum() == 580
assert customer_summary["order_count"].sum() == len(df)

assert top_2_customers["customer"].tolist() == ["Ali", "Ayse"]
assert top_2_customers["total_amount"].tolist() == [250, 250]

print("\nAll groupby checks passed!")
