select
    date_trunc('month', weather_date) as month,
    round(avg(temp_max)::numeric, 2) as avg_temp_max,
    round(avg(temp_min)::numeric, 2) as avg_temp_min,
    round(sum(precipitation)::numeric, 2) as total_precipitation
from {{ ref('stg_weather') }}
group by date_trunc('month', weather_date)
order by month