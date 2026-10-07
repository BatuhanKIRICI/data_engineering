-- ============================================================
-- Pattern 04: Top N per group
-- Database : Chinook (PostgreSQL)
-- Tables   : customer -> invoice, customer.support_rep_id -> employee
-- ============================================================

-- Problem:
-- For each support rep, return the top 3 customers by total revenue.
-- Not the top 3 overall: the top 3 INSIDE each rep's own list.
-- Target: one row per (rep, customer) for the customers ranked <= 3.

-- How to build it (always the same ladder):
--   1. What does one row mean?       -> one customer, with its rep
--   2. Compute the measure           -> GROUP BY customer (59 rows)
--   3. Rank inside each group        -> window function, PARTITION BY rep
--   4. Filter on the rank            -> wrap in a CTE, WHERE outside
--   5. Verify with counts


-- ------------------------------------------------------------
-- Solution (rank): everyone who sits in the top 3 ranks, ties included
-- ------------------------------------------------------------
with customer_revenue as (
    -- stage 1: one row per customer (the only GROUP BY in this query)
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.support_rep_id,
        sum(i.total) as total_revenue
    from customer c
    join invoice i
        on c.customer_id = i.customer_id
    group by
        c.customer_id,
        c.first_name,
        c.last_name,
        c.support_rep_id
),
ranked as (
    -- stage 2: rank each customer inside its rep's list (rows are kept)
    select
        e.first_name as rep_first_name,
        e.last_name  as rep_last_name,
        cr.customer_id,
        cr.first_name,
        cr.last_name,
        cr.total_revenue,
        rank() over (
            partition by e.employee_id
            order by cr.total_revenue desc      -- no unique tie-breaker on purpose
        ) as rnk
    from customer_revenue cr
    join employee e
        on cr.support_rep_id = e.employee_id
)
-- stage 3: filter. WHERE cannot see a window result in the same SELECT,
-- so the ranking lives in the CTE and the filter sits outside.
select *
from ranked
where rnk <= 3
order by rep_first_name, rnk, customer_id;
-- STATUS: ran on Chinook, 13 rows (Jane 4, Margaret 6, Steve 3), as expected.


-- ------------------------------------------------------------
-- Variant (row_number): exactly 3 rows per rep
-- Same query, only the window changes. customer_id breaks ties,
-- so from each tie one person is kept and the rest dropped.
-- ------------------------------------------------------------
-- rank() over (partition by e.employee_id order by cr.total_revenue desc)
--   becomes
-- row_number() over (partition by e.employee_id
--                    order by cr.total_revenue desc, cr.customer_id)
-- STATUS: expected 9 rows (3 per rep). NOT RUN YET on Chinook.


-- ------------------------------------------------------------
-- Result of the rank version (from Chinook)
-- ------------------------------------------------------------
-- Jane     45.62 (rank 1) x2 | 43.62 (rank 3) x2          -> 4 rows
-- Margaret 47.62 (1) | 40.62 (2) | 39.62 (3) x4           -> 6 rows
-- Steve    49.62 (1) | 46.62 (2) | 43.62 (3)              -> 3 rows


-- ------------------------------------------------------------
-- Notes
-- ------------------------------------------------------------
-- Which function? Decide what "top 3" means:
--   row_number <= 3 : exactly 3 rows per group, a tie at the cut is broken arbitrarily
--   rank       <= 3 : everyone with rank 1..3, can return more than 3 rows (gaps: 1,1,3)
--   dense_rank <= 3 : everyone in the top 3 DISTINCT values (no gaps: 1,1,2)
--
-- A unique tie-breaker (customer_id) in the ORDER BY of rank() turns it into
-- row_number: the ties disappear. Add it only when you want exactly N rows.
--
-- Why a CTE: the logical order is FROM/JOIN -> WHERE -> GROUP BY -> window -> SELECT -> ORDER BY.
-- WHERE runs before the window function exists, so "where rnk <= 3" in the
-- same SELECT fails (column "rnk" does not exist). Compute the rank in an inner
-- query, filter in the outer one.
--
-- GROUP BY here exists only in stage 1 (one row per customer). The window stage
-- needs no GROUP BY: PARTITION BY ranks rows without collapsing them.
--
-- Portability: this pattern works unchanged in Spark SQL. In engines with QUALIFY
-- (Snowflake, BigQuery) the filter can go directly on the window result.