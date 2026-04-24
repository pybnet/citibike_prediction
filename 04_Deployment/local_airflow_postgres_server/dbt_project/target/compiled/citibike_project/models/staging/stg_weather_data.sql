
-- ############################################
-- Staging model for Weather data
-- ############################################
with weather_base as (
    select *
    from "citibike_db"."public"."weather_data"
),

weather_cleaned as (
    select *
    from weather_base
    where time is not null
      and temp is not null
      and relative_humidity is not null
      and precipitation_total is not null
      and average_wind_speed is not null
      and coco is not null
      and coco_group is not null
)

select *
from weather_cleaned