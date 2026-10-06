-- ============================================================
-- Pattern 01: Top N customers by total spend
-- Database : Chinook (PostgreSQL)
-- Tables   : customer -> invoice -> invoice_line
-- ============================================================

-- Problem:
-- Find the top 5 customers by total expenditure.
-- Target shape: customer_id | first_name | last_name | total_expenditure (5 rows)


-- ------------------------------------------------------------
-- Approach 1: spending from invoice.total (invoice level)
-- ------------------------------------------------------------
select
    c.customer_id,
    c.first_name,
    c.last_name,
    sum(i.total) as total_expenditure
from customer c
join invoice i
    on c.customer_id = i.customer_id
group by
    c.customer_id,
    c.first_name,
    c.last_name
order by
    total_expenditure desc,
    c.customer_id          -- tie-breaker: keeps the result stable between runs
limit 5;


-- ------------------------------------------------------------
-- Approach 2: spending from invoice_line (line level)
-- unit_price * quantity, summed per customer
-- ------------------------------------------------------------
select
    c.customer_id,
    c.first_name,
    c.last_name,
    sum(il.unit_price * il.quantity) as total_expenditure
from invoice i
join customer c
    on i.customer_id = c.customer_id
join invoice_line il
    on i.invoice_id = il.invoice_id
group by
    c.customer_id,
    c.first_name,
    c.last_name
order by
    total_expenditure desc,
    c.customer_id
limit 5;


-- ------------------------------------------------------------
-- Reconciliation: customers where the two approaches disagree
-- STATUS: written but NOT RUN YET.
-- Expected: 0 rows. To prove the test can fail, temporarily change
-- the last condition from <> to = and confirm rows come back.
-- ------------------------------------------------------------
with via_invoice as (
    select customer_id, sum(total) as total_expenditure
    from invoice
    group by customer_id
),
via_lines as (
    select i.customer_id, sum(il.unit_price * il.quantity) as total_expenditure
    from invoice i
    join invoice_line il
        on il.invoice_id = i.invoice_id
    group by i.customer_id
)
select
    coalesce(a.customer_id, b.customer_id) as customer_id,
    a.total_expenditure as invoice_total,
    b.total_expenditure as line_total
from via_invoice a
full join via_lines b
    on a.customer_id = b.customer_id
where a.customer_id is null
   or b.customer_id is null
   or round(a.total_expenditure::numeric, 2) <> round(b.total_expenditure::numeric, 2);


-- ------------------------------------------------------------
-- Notes
-- ------------------------------------------------------------
-- Checked: the top 5 customers and their totals match across Approach 1 and 2.
-- Top 5 (by total): 6 Helena Holy 49.62 | 26 Richard Cunningham 47.62 |
--                   57 Luis Rojas 46.62 | 45 Ladislav Kovacs 45.62 | 46 Hugh O'Reilly 45.62
-- Tie: 4th and 5th customers have the same total (45.62). The customer_id
--      tie-breaker makes the order deterministic.
-- Checked (limit 10): both approaches return the same customers and totals.
-- 6th customer (Frank Ralston) has 43.62 < 45.62, so the top 5 is not cut inside a tie.
--   (4th and 5th tie at 45.62; the customer_id tie-breaker keeps the order stable.)
-- Full reconciliation over all customers: done in python/01_top_customers.py
--   (max difference 7.1e-15, float noise).
-- Gotcha: joining invoice_line and then summing invoice.total would multiply
--         each invoice total by its number of lines. Sum line-level columns instead.
-- Portability: PostgreSQL allows grouping by the primary key alone; listing
--              first_name and last_name too keeps the query valid in MySQL strict
--              mode, SQL Server, and Spark SQL.
