select count(*)
from weather_data;


select *
from weather_data
limit 5;


select weather,
       count(*) as days
from weather_data
group by weather
order by days desc;