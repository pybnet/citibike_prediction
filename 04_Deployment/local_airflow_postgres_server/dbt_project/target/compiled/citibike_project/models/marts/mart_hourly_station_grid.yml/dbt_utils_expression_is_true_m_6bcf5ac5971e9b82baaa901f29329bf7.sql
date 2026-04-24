

with meet_condition as (
    select * from "citibike_db"."public"."mart_hourly_station_grid" where 1=1
)

select
    *
from meet_condition

where not(year = extract(year from date_hour))

