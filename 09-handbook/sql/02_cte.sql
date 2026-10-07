-- ============================================================
-- Pattern 02: CTE (WITH) - aggregate in two stages
-- Database : Chinook (PostgreSQL)
-- Tables   : customer -> invoice, customer.support_rep_id -> employee
-- ============================================================

-- Problem:
-- Which support rep (employee) brings in the most revenue
-- through the customers they support?
-- Target shape: employee_id | first_name | last_name | customers | total_rev_emp
--               (one row per employee, highest revenue first)


-- ------------------------------------------------------------
-- Solution with a CTE
-- Stage 1 (CTE): revenue per customer      (one row per customer)
-- Stage 2      : revenue per employee      (one row per employee)
-- ------------------------------------------------------------
with customer_revenue as (
    select
        c.customer_id,
        c.support_rep_id,
        sum(i.total) as total_revenue
    from customer c
    join invoice i
        on c.customer_id = i.customer_id
    group by
        c.customer_id,
        c.support_rep_id
)
select
    e.employee_id,
    e.first_name,
    e.last_name,
    count(*)              as customers,
    sum(cr.total_revenue) as total_rev_emp
from employee e
join customer_revenue cr
    on e.employee_id = cr.support_rep_id
group by
    e.employee_id,
    e.first_name,
    e.last_name
order by total_rev_emp desc;


-- ------------------------------------------------------------
-- Cross-check: same result without a CTE
-- (one join chain, one GROUP BY). Results must be identical.
-- ------------------------------------------------------------
select
    e.employee_id,
    e.first_name,
    e.last_name,
    count(distinct c.customer_id) as customers,
    sum(i.total)                  as total_rev_emp
from employee e
join customer c
    on c.support_rep_id = e.employee_id
join invoice i
    on i.customer_id = c.customer_id
group by
    e.employee_id,
    e.first_name,
    e.last_name
order by total_rev_emp desc;

-- ------------------------------------------------------------
-- Variant: list the customers of each employee in one cell
-- Output is TEXT: good for display, not for filtering or summing.
-- string_agg is PostgreSQL-specific (MySQL: group_concat,
-- Spark: collect_list + concat_ws).
-- Checked: customers per employee = 21, 20, 18 -> total 59 = customer count.
-- ------------------------------------------------------------
select
    e.employee_id,
    e.first_name,
    e.last_name,
    count(*) as customers,
    string_agg(c.last_name, ', ' order by c.last_name) as customer_names
from employee e
join customer c
    on c.support_rep_id = e.employee_id
group by e.employee_id, e.first_name, e.last_name;

-- ------------------------------------------------------------
-- Sanity checks (run each, compare):
--   select sum(total) from invoice;      -- must equal the sum of total_rev_emp
--   select count(*) from customer;       -- must equal the sum of customers
-- STATUS: customer count check done (59). Revenue check (sum(total) = 2328.60)
--         and the no-CTE cross-check: NOT RUN YET.
-- ------------------------------------------------------------


-- ------------------------------------------------------------
-- Notes
-- ------------------------------------------------------------
-- A CTE lives only inside the one statement that defines it. It is not a
-- table: running "select ... from customer_revenue" on its own fails with
-- "relation does not exist". The WITH block must be in the same query.
-- In DBeaver a blank line can split the query in two; select the whole
-- statement and press Ctrl+Enter.
--
-- GROUP BY decides what one result row means:
--   group by customer_id  -> one row per customer
--   group by employee_id  -> one row per employee (customers are summed up)
--
-- Here the CTE is not strictly required (see the cross-check). It earns its
-- place when stage 1 is reused or filtered, e.g. "customers who spent more
-- than the average customer", where an aggregate has to be compared with
-- another aggregate.
--
-- Employees with no customers do not appear (inner join). Use LEFT JOIN
-- from employee if they should show up with 0.

