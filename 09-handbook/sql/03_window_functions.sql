-- ============================================================
-- Pattern 03: Window functions - rank customers inside each employee
-- Database : Chinook (PostgreSQL)
-- Tables   : customer -> invoice (customer.support_rep_id = the employee)
-- ============================================================

-- Problem:
-- For each support rep, rank their customers by total spend.
-- Keep one row per customer (do NOT collapse rows) and add a rank column.
-- Target shape: customer_id | first_name | last_name | support_rep_id | total_revenue | customer_rank


-- ------------------------------------------------------------
-- Query 1: row_number per employee (all employees)
-- ------------------------------------------------------------
select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.support_rep_id,
    sum(i.total) as total_revenue,
    row_number() over (
        partition by c.support_rep_id
        order by sum(i.total) desc, c.customer_id   -- customer_id: stable order inside ties
    ) as customer_rank
from customer c
join invoice i
    on c.customer_id = i.customer_id
group by
    c.customer_id,
    c.first_name,
    c.last_name,
    c.support_rep_id
order by
    c.support_rep_id,
    customer_rank;                                  -- rank order, not total_revenue ascending
-- STATUS: first version ran on Chinook (59 rows, rank restarts per employee).
--         This corrected version (tie-breaker + final ORDER BY) not re-run yet.


-- ------------------------------------------------------------
-- Query 2: row_number vs rank vs dense_rank (employee 4 only)
-- WHERE filters rows BEFORE the window functions are computed,
-- so ranks are computed inside employee 4's 20 customers only.
-- ------------------------------------------------------------
select
    c.customer_id,
    c.first_name,
    c.last_name,
    sum(i.total) as total_revenue,
    row_number() over (
        partition by c.support_rep_id
        order by sum(i.total) desc, c.customer_id
    ) as rn,
    rank() over (
        partition by c.support_rep_id
        order by sum(i.total) desc
    ) as rnk,
    dense_rank() over (
        partition by c.support_rep_id
        order by sum(i.total) desc
    ) as drnk
from customer c
join invoice i
    on c.customer_id = i.customer_id
where c.support_rep_id = 4
group by
    c.customer_id,
    c.first_name,
    c.last_name,
    c.support_rep_id
order by rn;
-- STATUS: ran on Chinook, output matched the expected table below.


-- ------------------------------------------------------------
-- Notes
-- ------------------------------------------------------------
-- Result of Query 2 (employee 4, 20 customers):
--   total   people  row_number   rank   dense_rank
--   47.62      1    1            1      1
--   40.62      1    2            2      2
--   39.62      4    3,4,5,6      3      3
--   38.62      2    7,8          7      4
--   37.62     12    9..20        9      5
--
-- row_number : one distinct number per row, ties broken by the second sort key
-- rank       : ties share a number, then the numbering JUMPS (3,3,3,3,7)
-- dense_rank : ties share a number, no jump (3,3,3,3,4)
--
-- GROUP BY vs PARTITION BY:
--   group by     -> collapses rows (one row per group)
--   partition by -> groups rows only for the calculation; every row stays
--
-- Why sum() can appear inside OVER (...):
--   Logical order is FROM/JOIN -> WHERE -> GROUP BY -> window functions -> ORDER BY.
--   GROUP BY has already produced one row per customer with its sum(),
--   so the window ranks those finished rows.
--
-- Always add a unique tie-breaker (customer_id) to row_number's ORDER BY,
-- otherwise rows with equal totals can swap places between runs.
--
-- A window function result cannot be used in WHERE directly
-- ("top N per group" needs a CTE or subquery around it) -> next entry.
--
-- Implication for "top N": for employee 4, top 4 with
--   rank       <= 4 returns 6 rows,  dense_rank <= 4 returns 8 rows,
--   row_number <= 4 returns 4 rows and drops 3 of the 4 people tied at 39.62.
--   (derived from the output above, not run)