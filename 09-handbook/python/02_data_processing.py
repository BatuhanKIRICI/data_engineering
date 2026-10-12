orders = [
    {"order_id": 101, "customer": "Ali", "amount": 120, "status": "completed"},
    {"order_id": 102, "customer": "Anna", "amount": 80, "status": "pending"},
    {"order_id": 103, "customer": "Ali", "amount": 50, "status": "completed"},
    {"order_id": 104, "customer": "John", "amount": 200, "status": "completed"},
]

print(type(orders))
print(len(orders))
print(orders[0])
print(orders[0]["amount"])


print("\nCompleted orders:")

for order in orders:
    if order["status"] == "completed":
        print(order["order_id"], order["customer"], order["amount"])

completed_orders = []

for order in orders:
    if order["status"] == "completed":
        completed_orders.append(order)

print("\nFiltered orders:")
print(completed_orders)
print("Completed order count:", len(completed_orders))

total_revenue = 0

for order in completed_orders:
    total_revenue += order["amount"]

print("\nTotal revenue:", total_revenue)


def calculate_revenue(order_list):
    total = 0

    for order in order_list:
        if order["status"] == "completed":
            total += order["amount"]

    return total


revenue = calculate_revenue(orders)

print("\nRevenue from function:", revenue)

assert revenue == 370

new_orders = [
    {"order_id": 201, "customer": "Lisa", "amount": 90, "status": "completed"},
    {"order_id": 202, "customer": "Max", "amount": 40, "status": "pending"},
    {"order_id": 203, "customer": "Lisa", "amount": 60, "status": "completed"},
]

new_revenue = calculate_revenue(new_orders)

print("\nNew revenue:", new_revenue)

assert new_revenue == 150

bad_order = {"order_id": 301, "customer": "Emma", "status": "completed"}

print("\nBad order:", bad_order)
print("Amount:", bad_order.get("amount"))

api_orders = [
    {"order_id": 401, "amount": 100, "status": "completed"},
    {"order_id": 402, "status": "completed"},
    {"order_id": 403, "amount": 75, "status": "completed"},
]

valid_orders = []
invalid_orders = []

for order in api_orders:
    amount = order.get("amount")

    if amount is None:
        invalid_orders.append(order)
    else:
        valid_orders.append(order)

print("\nValid orders:", valid_orders)
print("Invalid orders:", invalid_orders)

assert len(valid_orders) == 2


raw_amounts = ["100", "75.50", "abc", None]

clean_amounts = []
rejected_amounts = []

for amount in raw_amounts:
    try:
        parsed_amount = float(amount)
        clean_amounts.append(parsed_amount)
    except (ValueError, TypeError):
        rejected_amounts.append(amount)

print("\nClean amounts:", clean_amounts)
print("Rejected amounts:", rejected_amounts)


def clean_amount(order):
    raw_amount = order.get("amount")

    try:
        amount = float(raw_amount)
        return amount
    except (ValueError, TypeError):
        return None


print("\nAmount cleaning:")
print(clean_amount({"amount": "125.50"}))
print(clean_amount({"amount": "abc"}))
print(clean_amount({"order_id": 501}))


incoming_orders = [
    {"order_id": 501, "amount": "120.50", "status": "completed"},
    {"order_id": 502, "amount": "abc", "status": "completed"},
    {"order_id": 503, "amount": 80, "status": "completed"},
    {"order_id": 504, "status": "completed"},
]

cleaned_orders = []
rejected_orders = []

for order in incoming_orders:
    amount = clean_amount(order)

    if amount is None:
        rejected_orders.append(order)
    else:
        cleaned_order = order.copy()
        cleaned_order["amount"] = amount
        cleaned_orders.append(cleaned_order)

print("\nCleaned orders:", cleaned_orders)
print("Rejected orders:", rejected_orders)

total_revenue = 0

for order in cleaned_orders:
    total_revenue += order["amount"]

print("\nTotal revenue:", total_revenue)

assert len(cleaned_orders) == 2
assert len(rejected_orders) == 2
assert len(cleaned_orders) + len(rejected_orders) == len(incoming_orders)
assert total_revenue == 200.5
assert incoming_orders[0]["amount"] == "120.50"

print("All data quality checks passed!")
