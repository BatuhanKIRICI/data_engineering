select
    customer_id,
    customer_name,
    case
        when total_per_customer >= 1000 then 'VIP'
        when total_per_customer >= 500 then 'Premium'
        else 'Standard'
    end as segment
from {{ ref('revenue_per_customer') }}