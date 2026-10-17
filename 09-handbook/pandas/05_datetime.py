# ============================================================
# Pandas 05: Dates and times (to_datetime, filtering, .dt, daily/monthly sums)
#
# Problem:
#   Orders arrive with the date as text. Convert it to a real date type,
#   filter by date, split it into parts, and aggregate per day and per month.
#
# Pattern:
#   pd.to_datetime(col)                  text -> datetime64
#   df[df["order_date"] >= pd.Timestamp("2026-10-02")]   filter by date
#   df["order_date"].dt.year / .month / .day              date parts
#   df.groupby("order_date")["amount"].sum()               per day
#   df["order_date"].dt.to_period("M") + groupby           per month
#   .resample("D").sum()                 one row per calendar day (gaps = 0)
#
# Verified (assert):
#   - order_date is a datetime column after conversion
#   - ".dt" on a text column raises AttributeError
#   - date filter (>=) and a half-open range (>= start, < next month start)
#   - year / month / day parts
#   - daily sums: 5 days, total 700; resample fills the gaps: 34 days, 29 zero days
#   - monthly sums: Sep 60, Oct 550, Nov 90 (three months, so the grouping is
#     really tested); total 700
#   - an explicit format parses 02.10.2026 as 2 October (day.month.year)
#
# NOT covered:
#   - timezones (tz-aware timestamps), daylight saving time
#   - invalid dates (errors="coerce" / NaT), mixed formats
#   - large data
#
# Notes:
#   * The dtype prints as datetime64[us] or datetime64[ns] depending on the
#     pandas version. Both are fine; the asserts below do not depend on it.
#   * Filter by day with a range (>= start, < next day), not with ==,
#     because == only matches timestamps at exactly midnight.
#   * German dates (dd.mm.yyyy) are ambiguous: always pass format=.
#   * Days without orders do not appear in a plain groupby. Whether such a
#     day is 0 or "missing" is a business decision.
# ============================================================

import pandas as pd

orders = [
    {"order_id": 101, "order_date": "2026-10-01", "amount": 120},
    {"order_id": 102, "order_date": "2026-10-02", "amount": 80},
    {"order_id": 103, "order_date": "2026-10-02", "amount": 200},
    {"order_id": 104, "order_date": "2026-10-05", "amount": 150},
    {"order_id": 105, "order_date": "2026-09-30", "amount": 60},
    {"order_id": 106, "order_date": "2026-11-02", "amount": 90},
]

df = pd.DataFrame(orders)

# --- 1. Text -> datetime ---
print("Before conversion:")
print(df.dtypes)

df["order_date"] = pd.to_datetime(df["order_date"])

print("\nAfter conversion:")
print(df.dtypes)

assert pd.api.types.is_datetime64_any_dtype(df["order_date"])

# .dt only works on datetime columns, not on text
try:
    pd.Series(["2026-10-01"]).dt.year
except AttributeError:
    pass
else:
    raise AssertionError(".dt should fail on a text column")

# Day.month.year text needs an explicit format
assert pd.to_datetime("02.10.2026", format="%d.%m.%Y") == pd.Timestamp("2026-10-02")

# --- 2. Filter by date ---
cutoff_date = pd.Timestamp("2026-10-02")

recent_orders = df[df["order_date"] >= cutoff_date].copy()

print("\nOrders from October 2 onward:")
print(recent_orders)

assert recent_orders["order_id"].tolist() == [102, 103, 104, 106]
assert recent_orders["amount"].sum() == 520

# A range inside October: >= start and < start of the next month
october_from_2nd = df[
    (df["order_date"] >= cutoff_date) & (df["order_date"] < pd.Timestamp("2026-11-01"))
].copy()

print("\nOctober orders from the 2nd:")
print(october_from_2nd)

assert october_from_2nd["order_id"].tolist() == [102, 103, 104]
assert october_from_2nd["amount"].sum() == 430

# --- 3. Date parts with .dt ---
df["year"] = df["order_date"].dt.year
df["month"] = df["order_date"].dt.month
df["day"] = df["order_date"].dt.day

print("\nOrders with date parts:")
print(df)

assert df["year"].tolist() == [2026, 2026, 2026, 2026, 2026, 2026]
assert df["month"].tolist() == [10, 10, 10, 10, 9, 11]
assert df["day"].tolist() == [1, 2, 2, 5, 30, 2]

# --- 4. Daily sales ---
daily_sales = df.groupby("order_date")["amount"].sum().reset_index()

print("\nDaily sales:")
print(daily_sales)

assert len(daily_sales) == 5
assert daily_sales["amount"].sum() == df["amount"].sum()
assert daily_sales["amount"].sum() == 700

# Days without orders are missing above; resample adds every calendar day
daily_full = df.set_index("order_date")["amount"].resample("D").sum()

assert len(daily_full) == 34
assert daily_full.sum() == 700
assert (daily_full == 0).sum() == 29

# --- 5. Monthly sales ---
df["order_month"] = df["order_date"].dt.to_period("M")

monthly_sales = df.groupby("order_month")["amount"].sum().reset_index()

print("\nMonthly sales:")
print(monthly_sales)

assert monthly_sales["order_month"].astype(str).tolist() == [
    "2026-09",
    "2026-10",
    "2026-11",
]
assert monthly_sales["amount"].tolist() == [60, 550, 90]
assert monthly_sales["amount"].sum() == df["amount"].sum()

print("\nAll datetime checks passed!")
