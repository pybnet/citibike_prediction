{{ config(materialized='view') }}

-- ############################################
-- Staging model for Citibike trips (raw)
-- ############################################
with trips_base as (
    select *
    from {{ source('raw', 'citibike_trips_raw') }}
),

trips_cleaned as (
    select *
    from trips_base
    where started_at is not null
      and ended_at is not null
      and start_station_id is not null
      and end_station_id is not null
      and rideable_type is not null
      and member_casual is not null
)

select *
from trips_cleaned