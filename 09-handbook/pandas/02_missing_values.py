import pandas as pd

orders = [
    {"order_id": 101, "customer": "Ali", "amount": 120},
    {"order_id": 102, "customer": None, "amount": 80},
    {"order_id": 103, "customer": "Ayse", "amount": None},
    {"order_id": 104, "customer": "Mehmet", "amount": 200},
]

df = pd.DataFrame(orders)

print("Original data:")
print(df)

# print("\nMissing values per column:")
# print(df.isna().sum())

df["customer"] = df["customer"].fillna("Unknown")

assert df["customer"].isna().sum() == 0

# print("\nAfter filling customer names:")
# print(df)

# print("\nMissing values:")
# print(df.isna().sum())

valid_orders = df[df["amount"].notna()].copy()

invalid_orders = df[df["amount"].isna()].copy()

print("\nValid orders:")
print(valid_orders)

print("\nInvalid orders:")
print(invalid_orders)

source_count = len(df)
valid_count = len(valid_orders)
invalid_count = len(invalid_orders)

print("\nSource rows:", source_count)
print("Valid rows:", valid_count)
print("Invalid rows:", invalid_count)

assert source_count == valid_count + invalid_count

print("Count reconciliation passed!")

source_ids = set(df["order_id"])
valid_ids = set(valid_orders["order_id"])
invalid_ids = set(invalid_orders["order_id"])

assert valid_ids | invalid_ids == source_ids
assert valid_ids & invalid_ids == set()

print("\nID reconciliation passed!")
