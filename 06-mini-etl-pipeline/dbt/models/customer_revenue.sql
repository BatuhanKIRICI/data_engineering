select
    customer,
    sum(amount) as total_amount
from mini_orders
group by customer