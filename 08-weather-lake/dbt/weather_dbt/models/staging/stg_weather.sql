select
    date::date as weather_date,
    precipitation,
    temp_max,
    temp_min,
    wind,
    weather
from weather_data