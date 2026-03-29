
  create view "citibike_db"."public"."stg_citibike_stations__dbt_tmp"
    
    
  as (
    

-- ############################################
-- Staging model for Citibike stations
-- ############################################
with stations_base as (
    select *
    from "citibike_db"."public"."citibike_stations"
),

stations_cleaned as (
    select *
    from stations_base
    where short_name is not null
      and station_id is not null
      and name is not null
      and capacity is not null
      and lat is not null
      and lon is not null
)

select *
from stations_cleaned
  );