
    
    

select
    date_hour as unique_field,
    count(*) as n_records

from "citibike_db"."public"."mart_weather_hourly"
where date_hour is not null
group by date_hour
having count(*) > 1


