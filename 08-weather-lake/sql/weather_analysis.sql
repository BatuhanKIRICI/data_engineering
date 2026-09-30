select
	count(*)
from
	weather_data;


select
	*
from
	weather_data
limit 5;


select
	weather,
	count(*) as days
from
	weather_data
group by
	weather
order by
	days desc;


select
	date_trunc('month', date::date) as month,
	round(avg(temp_max)::numeric, 2) as avg_temp_max,
	round(avg(temp_min)::numeric, 2) as avg_temp_min,
	round(sum(precipitation)::numeric, 2) as total_precipitation
from
	weather_data
group by
	date_trunc('month', date::date)
order by
	month;


select
	extract(year from date::date) as year,
	round(avg(temp_max)::numeric, 2) as avg_temp_max,
	round(sum(precipitation)::numeric, 2) as total_precipitation
from
	weather_data
group by
	extract(year from date::date)
order by
	year;




select
	date,
	precipitation,
	lag(precipitation) over (
	order by
		date::date
	) as previous_day,
	precipitation - lag(precipitation) over (
	order by
		date::date
	) as change
from
	weather_data
order by
	date::date;


select
	date,
	temp_max,
	round(
        avg(temp_max) over (
            order by date::date
            rows between 6 preceding and current row
        )::numeric,
        2
    ) as moving_avg_7d
from
	weather_data
order by
	date::date;


select
	date,
	temp_min,
	round(
        avg(temp_min) over (
            order by date::date
            rows between 6 preceding and current row
        )::numeric,
        2
    ) as moving_avg_7d
from
	weather_data
order by
	date::date;





















































