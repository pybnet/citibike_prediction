
  
    

  create  table "citibike_db"."public"."stg_biketrip_hourly_drops__dbt_tmp"
  
  
    as
  
  (
    

WITH base AS (
    SELECT
        end_station_id AS station_id,
        DATE_TRUNC('day', ended_at) AS date,
        EXTRACT(hour FROM ended_at) AS hour
    FROM "citibike_db"."public"."int_biketrip_features"
)

SELECT
    station_id,
    date,
    hour,
    COUNT(*) AS num_bikes_dropped
FROM base
GROUP BY 1,2,3
  );
  