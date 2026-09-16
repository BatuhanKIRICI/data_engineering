select
	dc.customer_id,
	customer_name,
	sum(total_amount) total_per_customer
from
	fact_sales fs
join dim_customer dc on
	fs.customer_id = dc.customer_id
group by
	dc.customer_id,
	dc.customer_name
order by
	customer_id
	asc
