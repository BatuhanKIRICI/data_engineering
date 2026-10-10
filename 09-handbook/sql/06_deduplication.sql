-- ============================================================
-- Pattern 06: Deduplication / Latest record per group
-- Database: Chinook (PostgreSQL)
--
-- Problem:
--   A) Same order arrives several times (status updates).
--      Keep only the newest version of each order.
--   B) Each customer has many invoices.
--      Return only the latest invoice per customer.
--
-- Pattern:
--   row_number() over (partition by <group> order by <time> desc, <id> desc)
--   then filter rn = 1 in an outer layer.
--
-- Verified on Chinook (PostgreSQL):
--   synthetic order_updates: 5 rows -> 2 (101 delivered, 102 shipped)
--   customers = customers_with_invoice = 59
--   latest invoice per customer: 59 rows
--   cross-checked against group by + max(invoice_date): mismatches = 0
-- ============================================================


-- ------------------------------------------------------------
-- A) Synthetic example: keep the newest update per order
-- ------------------------------------------------------------
with order_updates (order_id, status, updated_at) as (
    values
        (101, 'pending',   timestamp '2025-01-10 10:00:00'),
        (101, 'shipped',   timestamp '2025-01-10 11:00:00'),
        (102, 'pending',   timestamp '2025-01-10 10:30:00'),
        (101, 'delivered', timestamp '2025-01-10 14:00:00'),
        (102, 'shipped',   timestamp '2025-01-10 12:00:00')
),
ranked_updates as (
    select
        order_id,
        status,
        updated_at,
        row_number() over (
            partition by order_id
            order by updated_at desc
        ) as rn
    from order_updates
)
select order_id, status, updated_at
from ranked_updates
where rn = 1
order by order_id;
-- Expected: 101 delivered (14:00) | 102 shipped (12:00)
-- Note: "latest" is defined by updated_at, not by physical row order.


-- ------------------------------------------------------------
-- B) Chinook: latest invoice per customer
-- ------------------------------------------------------------
with ranked_invoices as (
    select
        invoice_id,
        customer_id,
        invoice_date,
        total,
        row_number() over (
            partition by customer_id
            order by invoice_date desc, invoice_id desc   -- id = tie-breaker
        ) as rn
    from invoice
)
select customer_id, invoice_id, invoice_date, total
from ranked_invoices
where rn = 1
order by customer_id;
-- Expected: 59 rows (one per customer)


-- ------------------------------------------------------------
-- Verification
-- ------------------------------------------------------------

-- 1) Every customer has an invoice (both must be 59)
select
    (select count(*) from customer)                   as customers,
    (select count(distinct customer_id) from invoice) as customers_with_invoice;

-- 2) Independent method: group by + max(invoice_date).
--    mismatches must be 0.
with ranked_invoices as (
    select
        invoice_id, customer_id, invoice_date,
        row_number() over (
            partition by customer_id
            order by invoice_date desc, invoice_id desc
        ) as rn
    from invoice
),
latest as (
    select customer_id, invoice_date from ranked_invoices where rn = 1
),
check_max as (
    select customer_id, max(invoice_date) as invoice_date
    from invoice
    group by customer_id
)
select count(*) as mismatches
from latest l
join check_max c using (customer_id)
where l.invoice_date <> c.invoice_date;


-- ------------------------------------------------------------
-- Notes
-- ------------------------------------------------------------
-- * row_number (not rank): we want exactly one row per group, no ties.
-- * Tie-breaker (invoice_id desc): same date twice would make the
--   result non-deterministic without it.
-- * max() gives only the latest date; row_number keeps the whole row
--   (invoice_id, total, ...). Use row_number when you need the row.
-- * Same two-layer shape as Pattern 04: produce rn, then filter.
--
-- Common mistakes:
--   - where rn = 1 in the same select that creates rn (rn doesn't exist yet)
--   - no tie-breaker in order by
--   - order by asc instead of desc (returns the OLDEST record)
