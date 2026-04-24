
  create view "citibike_db"."public"."int_biketrip_hourly_net_flow_dedup__dbt_tmp"
    
    
  as (
    

with ranked as (
    select
        *,
        row_number() over (
            partition by station_id, date_hour
            order by net_flow desc  -- choose the logic to keep one row if duplicates exist
        ) as rn
    from "citibike_db"."public"."int_biketrip_hourly_net_flow"
)

select
    station_id,
    date_hour,
    num_bikes_taken,
    num_bikes_dropped,
    net_flow
from ranked
where rn = 1
  );