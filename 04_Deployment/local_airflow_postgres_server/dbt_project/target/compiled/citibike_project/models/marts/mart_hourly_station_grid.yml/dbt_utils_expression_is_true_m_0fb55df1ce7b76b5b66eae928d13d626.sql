

with meet_condition as (
    select * from "citibike_db"."public"."mart_hourly_station_grid" where 1=1
)

select
    *
from meet_condition

where not(net_flow = coalesce(num_bikes_taken, 0) - coalesce(num_bikes_dropped, 0))

