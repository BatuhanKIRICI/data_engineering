-- ============================================================
-- Pattern 05: Time-series analytics (monthly revenue, MoM)
-- Database: Chinook (PostgreSQL)
--
-- Problem:
--   Monthly revenue, the previous month's revenue,
--   the difference and the month-over-month % change.
--
-- Pattern:
--   date_trunc('month') + group by  -> one row per month
--   lag() over (order by month)     -> previous month (second CTE)
--   100.0 * (x - prev) / nullif(prev, 0) -> safe % change
--
-- Verified on Chinook (PostgreSQL):
--   month_count = 60 (2021-01 .. 2025-12)
--   invoice_total = monthly_total = 2328.60
--   lag checked by hand: 2021-02 prev_revenue = 35.64
--   MoM spot checks: 2022-01 39.87 | 2023-11 -36.84
--                    2023-12 58.33 | 2025-12 -22.17
--   Data is synthetic: most months are exactly 37.62, so MoM is mostly 0.00.
-- ============================================================


-- ------------------------------------------------------------
-- Solution
-- ------------------------------------------------------------
with monthly_revenue as (
    select
        date_trunc('month', invoice_date)::date as month,
        sum(total)                              as revenue
    from invoice
    group by date_trunc('month', invoice_date)
),
with_prev as (
    select
        month,
        revenue,
        lag(revenue) over (order by month) as prev_revenue
    from monthly_revenue
)
select
    month,
    revenue,
    prev_revenue,
    revenue - prev_revenue as diff,
    round(100.0 * (revenue - prev_revenue) / nullif(prev_revenue, 0), 2) as mom_pct
from with_prev
order by month;
-- Expected: 60 rows. First row has prev_revenue / diff / mom_pct = NULL.


-- ------------------------------------------------------------
-- Verification (reconciliation)
-- ------------------------------------------------------------
-- Row count and totals must match: 60 | 2328.60 | 2328.60
with monthly_revenue as (
    select
        date_trunc('month', invoice_date)::date as month,
        sum(total)                              as revenue
    from invoice
    group by date_trunc('month', invoice_date)
)
select
    count(*)                              as month_count,
    sum(revenue)                          as monthly_total,
    (select sum(total) from invoice)      as invoice_total
from monthly_revenue;


-- ------------------------------------------------------------
-- Notes
-- ------------------------------------------------------------
-- * group by first, lag second: lag works on the monthly rows,
--   not on the raw invoices.
-- * lag() is a window function, so it needs its own layer (CTE);
--   the % calculation then uses prev_revenue from that layer.
-- * First month has no previous month -> NULL, that is correct.
-- * nullif(prev, 0) turns a zero divisor into NULL instead of an error.
-- * 100.0 (not 100) forces numeric division; round() on numeric is fine.
-- * date_trunc('month', ...) keeps month as the row identity:
--   GROUP BY defines what one row means.
--
-- Common mistakes:
--   - lag() in the same select as group by on raw invoices
--     (compares invoices, not months)
--   - integer division (100 instead of 100.0)
--   - forgetting order by inside over() -> lag has no defined "previous"
--   - months with no invoices simply do not appear (no zero-fill here)
