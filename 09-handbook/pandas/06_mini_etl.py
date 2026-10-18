# ============================================================
# Pandas 06: Mini ETL (validate -> join -> report -> reconcile)
#
# Tested with pandas 3.0.5
#
# Problem:
#   Raw orders arrive with a missing amount and an unknown customer.
#   Split them into valid / rejected, join customers, build a daily sales
#   report from the good rows, and prove that no order got lost or duplicated.
#
# Rules (checked in this order, each order lands in exactly one group):
#   1. invalid   : amount is missing           -> invalid_orders
#   2. unmatched : amount ok, customer unknown -> unmatched_orders
#   3. matched   : amount ok, customer known   -> matched_orders -> report
#
# Pattern:
#   orders[orders["amount"].notna()]                 validate (mask + .copy())
#   pd.merge(..., how="left", indicator=True,
#            validate="many_to_one")                 join + detect unmatched
#   merged["_merge"] == "both" / "left_only"         split by join result
#   matched.groupby("order_date")["amount"].sum()    daily report
#   set(ids) | & + control totals                    reconciliation
#
# Verified (assert):
#   - 6 source rows / 600 total (hand-calculated)
#   - invalid [105], unmatched [104], matched [101, 102, 103, 106]
#   - daily report: 10-01 120, 10-02 280, 10-05 50, total 450 (hand-calculated)
#   - every source id is in exactly one group (union == source, no overlap)
#   - money: matched 450 + unmatched 150 == source total 600
#     (the invalid order has no amount, so it adds nothing)
#
# NOT covered:
#   - a wrong filter that still splits cleanly (e.g. 103 tagged unmatched
#     instead of 104): only the explicit id asserts above catch that
#   - source_rows / source_total come from the same table here; in a real
#     pipeline take them from the source system (e.g. SQL COUNT(*) / SUM)
#   - changed amounts for known ids, duplicate customers in real data,
#     large data, writing the result somewhere
#
# Notes:
#   * amount prints as 120.0 because None made the column float.
#   * Rejected rows are kept (invalid_orders, unmatched_orders), not dropped,
#     so someone can fix them. Excluding unmatched orders from the report is
#     a business rule chosen for this example.
#   * Missing string values print as NaN in pandas 3 (None in older versions).
# ============================================================

import pandas as pd

customers = pd.DataFrame(
    [
        {"customer_id": 1, "name": "Ali"},
        {"customer_id": 2, "name": "Ayse"},
        {"customer_id": 3, "name": "Mehmet"},
    ]
)

orders = pd.DataFrame(
    [
        {"order_id": 101, "customer_id": 1, "order_date": "2026-10-01", "amount": 120},
        {"order_id": 102, "customer_id": 2, "order_date": "2026-10-02", "amount": 80},
        {"order_id": 103, "customer_id": 1, "order_date": "2026-10-02", "amount": 200},
        {"order_id": 104, "customer_id": 4, "order_date": "2026-10-03", "amount": 150},
        {"order_id": 105, "customer_id": 3, "order_date": "2026-10-04", "amount": None},
        {"order_id": 106, "customer_id": 2, "order_date": "2026-10-05", "amount": 50},
    ]
)

# Control totals taken at the start, before any step touches the data
source_rows = len(orders)
source_total = orders["amount"].sum()

assert source_rows == 6
assert source_total == 600
assert orders["order_id"].is_unique

# --- 1. Validate: split by missing amount ---
valid_orders = orders[orders["amount"].notna()].copy()
invalid_orders = orders[orders["amount"].isna()].copy()

print("Valid orders:")
print(valid_orders)
print("\nInvalid orders (missing amount):")
print(invalid_orders)

assert len(valid_orders) == 5
assert len(invalid_orders) == 1
assert len(valid_orders) + len(invalid_orders) == source_rows
assert valid_orders["amount"].sum() == 600
assert invalid_orders["order_id"].tolist() == [105]

# --- 2. Join customers, split by join result ---
merged_orders = pd.merge(
    valid_orders,
    customers,
    on="customer_id",
    how="left",
    indicator=True,
    validate="many_to_one",
)

matched_orders = merged_orders[merged_orders["_merge"] == "both"].copy()
unmatched_orders = merged_orders[merged_orders["_merge"] == "left_only"].copy()

print("\nMerged orders:")
print(merged_orders)
print("\nUnmatched orders (customer not found):")
print(unmatched_orders)

assert len(merged_orders) == len(valid_orders)
assert len(matched_orders) == 4
assert len(unmatched_orders) == 1
assert len(matched_orders) + len(unmatched_orders) == len(valid_orders)
assert matched_orders["order_id"].tolist() == [101, 102, 103, 106]
assert unmatched_orders["order_id"].tolist() == [104]

# --- 3. Daily sales report (matched orders only) ---
matched_orders["order_date"] = pd.to_datetime(matched_orders["order_date"])

daily_sales = matched_orders.groupby("order_date")["amount"].sum().reset_index()

print("\nDaily sales:")
print(daily_sales)

assert len(daily_sales) == 3
assert daily_sales["amount"].tolist() == [120, 280, 50]
assert daily_sales["amount"].sum() == 450
assert daily_sales["amount"].sum() == matched_orders["amount"].sum()

# --- 4. Reconciliation: where did every source order go? ---
source_ids = set(orders["order_id"])
matched_ids = set(matched_orders["order_id"])
unmatched_ids = set(unmatched_orders["order_id"])
invalid_ids = set(invalid_orders["order_id"])

# No order lost
assert matched_ids | unmatched_ids | invalid_ids == source_ids
# No order in two groups
assert matched_ids & unmatched_ids == set()
assert matched_ids & invalid_ids == set()
assert unmatched_ids & invalid_ids == set()
# Row count (alone this would miss "one duplicated + one lost")
assert len(matched_orders) + len(unmatched_orders) + len(invalid_orders) == source_rows

# Money: invalid order has no amount, so matched + unmatched must equal the source total
assert matched_orders["amount"].sum() + unmatched_orders["amount"].sum() == source_total

print("\nSummary:")
print(f"  source orders : {source_rows} (total {source_total:.0f})")
print(
    f"  reported      : {len(matched_orders)} (total {matched_orders['amount'].sum():.0f})"
)
print(
    f"  unmatched     : {len(unmatched_orders)} (total {unmatched_orders['amount'].sum():.0f})"
)
print(f"  invalid       : {len(invalid_orders)} (amount missing)")

print("\nAll mini ETL checks passed!")
