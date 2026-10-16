# ============================================================
# Pandas 01: Filtering and selecting (rows, columns, loc/iloc)
#
# Patterns:
#   df[df["amount"] > 100]                     row filter
#   (cond1) & (cond2),  (cond1) | (cond2)      AND / OR, parentheses required
#   df[["col"]] -> DataFrame, df["col"] -> Series
#   df.loc[mask, ["col"]]                      filter rows + select columns
#   .loc = label, .iloc = position (differ after filtering)
#
# Verified: row counts and selected ids via assert.
# Not covered: NaN in filtered columns, large data, assignment on filtered data.
# ============================================================

import pandas as pd

orders = [
    {"order_id": 101, "amount": 80},
    {"order_id": 102, "amount": 150},
    {"order_id": 103, "amount": 100},
    {"order_id": 104, "amount": 220},
]

df = pd.DataFrame(orders)

# print("All orders:")
# print(df)

large_orders = df[df["amount"] > 100]

# print("\nOrders above 100:")
# print(large_orders)

filtered_orders = df[(df["amount"] > 100) & (df["amount"] < 200)]

# print("\nOrders between 100 and 200:")
# print(filtered_orders)

special_orders = df[(df["amount"] < 100) | (df["amount"] > 200)]

# print("\nOrders below 100 OR above 200:")
# print(special_orders)

order_ids = df[["order_id"]]

# print("\nOnly order IDs:")
# print(order_ids)
# print(type(df["order_id"]))
# print(type(df[["order_id"]]))

series = df["order_id"]
dataframe = df[["order_id"]]

# print("Series shape:", series.shape)
# print("DataFrame shape:", dataframe.shape)

filtered_ids = df.loc[df["amount"] > 100, ["order_id"]]

# print("\nFiltered order IDs:")
# print(filtered_ids)

# print("\nLOC result:", df.loc[1, "amount"])
# print("ILOC result:", df.iloc[1, 1])

filtered_df = df[df["amount"] > 100]

# print("\nFiltered DataFrame:")
# print(filtered_df)

# print("First row with iloc:")
# print(filtered_df.iloc[0])

print("\nLOC index 1:")
print(filtered_df.loc[1])

print("\nILOC position 1:")
print(filtered_df.iloc[1])

assert len(large_orders) == 2
assert len(filtered_orders) == 1
assert len(special_orders) == 2

assert filtered_ids["order_id"].tolist() == [102, 104]

print("\nAll filtering checks passed!")
