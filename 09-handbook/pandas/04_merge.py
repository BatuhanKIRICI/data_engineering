# ============================================================
# Pandas 04: merge (inner / left), indicator, validate
#
# Problem:
#   Join orders to customers without losing or duplicating orders,
#   and find the orders that have no matching customer.
#
# Pattern:
#   pd.merge(orders, customers, on="customer_id", how="inner" | "left")
#   indicator=True  -> adds _merge: both | left_only | right_only
#   validate="many_to_one" -> raises MergeError if the right key repeats
#   reconcile after the join: rows, sum of amount, order ids
#
# Verified (assert):
#   - inner join drops order 104 (customer_id 4 has no customer)
#   - left join keeps all 4 orders; the only unmatched order is 104
#   - validate="many_to_one" passes on clean data and raises on a
#     customers table with a duplicated customer_id
#   - without validate, the duplicate gives 6 rows and 870 instead of
#     4 rows and 550 (silently wrong totals)
#   - rows, total amount (550, also hand-calculated) and order ids are
#     preserved by the left join; matched + unmatched == source
#
# NOT covered:
#   - NaN keys, different key names (left_on/right_on), suffixes
#   - many_to_many joins, large data
#   - what to do with unmatched rows (here: only detected, not quarantined)
#
# Notes:
#   * inner join silently loses rows; left join keeps them (NaN on the right)
#   * NaN in a joined column can mean "no match" or "matched but empty";
#     only _merge tells them apart
#   * validate checks key uniqueness only; it does not report unmatched rows
#   * in a real pipeline, re-raise the MergeError instead of printing it
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
        {"order_id": 101, "customer_id": 1, "amount": 120},
        {"order_id": 102, "customer_id": 2, "amount": 80},
        {"order_id": 103, "customer_id": 1, "amount": 200},
        {"order_id": 104, "customer_id": 4, "amount": 150},
    ]
)

print("Customers:")
print(customers)

print("\nOrders:")
print(orders)

# --- 1. Inner join: only orders with a customer ---
inner_result = pd.merge(orders, customers, on="customer_id", how="inner")

print("\nInner join result:")
print(inner_result)

assert len(inner_result) == 3
assert set(inner_result["order_id"]) == {101, 102, 103}
# What did the inner join lose?
assert set(orders["order_id"]) - set(inner_result["order_id"]) == {104}

# --- 2. Left join: all orders, with indicator and validate in one merge ---
merge_check = pd.merge(
    orders,
    customers,
    on="customer_id",
    how="left",
    indicator=True,
    validate="many_to_one",
)

print("\nMerge indicator:")
print(merge_check)

# The left join result without the helper column
left_result = merge_check.drop(columns="_merge")

print("\nLeft join result:")
print(left_result)

assert len(left_result) == len(orders)
assert set(left_result.loc[left_result["name"].isna(), "order_id"]) == {104}

# --- 3. Unmatched orders (detected via _merge, not via NaN) ---
unmatched_orders = merge_check[merge_check["_merge"] == "left_only"].copy()

print("\nUnmatched orders:")
print(unmatched_orders)

assert unmatched_orders["order_id"].tolist() == [104]

# --- 4. validate: duplicate customer_id must be caught ---
duplicate_customers = pd.DataFrame(
    [
        {"customer_id": 1, "name": "Ali"},
        {"customer_id": 1, "name": "Ali Duplicate"},
        {"customer_id": 2, "name": "Ayse"},
        {"customer_id": 3, "name": "Mehmet"},
    ]
)

try:
    pd.merge(
        orders,
        duplicate_customers,
        on="customer_id",
        how="left",
        validate="many_to_one",
    )
except pd.errors.MergeError as error:
    print("\nDuplicate customer ID detected!")
    print("Error:", error)
else:
    # If validate did not raise, this test itself must fail
    raise AssertionError("validate did not catch the duplicate customer_id")

# Same data without validate: no error, but the result is wrong
no_validate = pd.merge(orders, duplicate_customers, on="customer_id", how="left")

print(
    "\nWithout validate: rows =",
    len(no_validate),
    "| total =",
    no_validate["amount"].sum(),
)

assert len(no_validate) == 6
assert no_validate["amount"].sum() == 870

# --- 5. Reconciliation of the left join against the source ---
matched_orders = merge_check[merge_check["_merge"] == "both"].copy()

assert len(merge_check) == len(orders)
assert merge_check["amount"].sum() == orders["amount"].sum() == 550
assert len(matched_orders) + len(unmatched_orders) == len(orders)
assert set(merge_check["order_id"]) == set(orders["order_id"])

print("\nAll merge checks passed!")
