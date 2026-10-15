# ============================================================
# Pattern 07: Incremental load + idempotency (pure Python)
#
# Problem:
#   Source sends orders every day, including ones already loaded.
#   Load only new orders; running the load twice must not
#   create duplicates.
#
# Pattern:
#   known_ids set -> "order_id not in known_ids" -> append ->
#   add id to the set immediately (also blocks duplicates
#   inside the incoming batch).
#
# Verified:
#   - first load 2, second load 0, rows stay 5
#   - count reconciliation (3 + 2 = 5)
#   - ID reconciliation (expected id set == actual id set, no duplicates)
#   - duplicate id inside source loaded once
#
# NOT verified / not handled:
#   - same id with changed amount (needs upsert / updated_at watermark)
#   - PostgreSQL side (primary key, ON CONFLICT, transactions)
#   - original_count is hardcoded; in a real pipeline it is queried
#     from the target before loading
# ============================================================

existing_orders = [
    {"order_id": 101, "amount": 120},
    {"order_id": 102, "amount": 80},
    {"order_id": 103, "amount": 50},
]

incoming_orders = [
    {"order_id": 102, "amount": 80},
    {"order_id": 103, "amount": 50},
    {"order_id": 104, "amount": 200},
    {"order_id": 105, "amount": 150},
]

existing_ids = set()

for order in existing_orders:
    existing_ids.add(order["order_id"])

print("Existing IDs:", existing_ids)

new_orders = []

for order in incoming_orders:
    if order["order_id"] not in existing_ids:
        new_orders.append(order)

print("\nNew orders:")
for order in new_orders:
    print(order)

existing_orders.extend(new_orders)

print("\nOrders after first load:", len(existing_orders))

existing_ids = set()

for order in existing_orders:
    existing_ids.add(order["order_id"])

second_run_new_orders = []

for order in incoming_orders:
    if order["order_id"] not in existing_ids:
        second_run_new_orders.append(order)

print("New orders on second run:", len(second_run_new_orders))

assert len(existing_orders) == 5
assert len(second_run_new_orders) == 0

print("Idempotency check passed!")

original_count = 3
new_count = len(new_orders)
actual_count = len(existing_orders)

expected_count = original_count + new_count

print("\nExpected count:", expected_count)
print("Actual count:", actual_count)

assert actual_count == expected_count

print("Reconciliation passed!")


def load_new_orders(existing, incoming):
    existing_ids = set()

    for order in existing:
        existing_ids.add(order["order_id"])

    loaded_count = 0

    for order in incoming:
        if order["order_id"] not in existing_ids:
            existing.append(order.copy())
            existing_ids.add(order["order_id"])
            loaded_count += 1

    return loaded_count


test_orders = [
    {"order_id": 101, "amount": 120},
    {"order_id": 102, "amount": 80},
    {"order_id": 103, "amount": 50},
]

first_load = load_new_orders(test_orders, incoming_orders)
count_after_first_load = len(test_orders)

second_load = load_new_orders(test_orders, incoming_orders)
count_after_second_load = len(test_orders)

print("\nFirst load:", first_load)
print("Second load:", second_load)
print("Rows after first load:", count_after_first_load)
print("Rows after second load:", count_after_second_load)

assert first_load == 2
assert second_load == 0
assert count_after_first_load == count_after_second_load == 5

print("Real idempotency check passed!")

expected_ids = {101, 102, 103, 104, 105}

actual_ids = set()

for order in test_orders:
    actual_ids.add(order["order_id"])

assert actual_ids == expected_ids
assert len(actual_ids) == len(test_orders)

print("\nID reconciliation passed!")

dup_test = []

duplicate_incoming = [
    {"order_id": 106, "amount": 100},
    {"order_id": 106, "amount": 100},
]

loaded = load_new_orders(dup_test, duplicate_incoming)

print("Loaded:", loaded)
print("Target rows:", len(dup_test))

assert loaded == 1
assert len(dup_test) == 1

print("\nSource duplicate test passed!")
