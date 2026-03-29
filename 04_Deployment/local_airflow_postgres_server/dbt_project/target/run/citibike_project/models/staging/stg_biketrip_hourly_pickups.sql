
  
    

  create  table "citibike_db"."public"."stg_biketrip_hourly_pickups__dbt_tmp"
  
  
    as
  
  (
    

WITH base AS (
    SELECT
        start_station_id AS station_id,
        DATE_TRUNC('day', started_at) AS date,
        EXTRACT(hour FROM started_at) AS hour
    FROM "citibike_db"."public"."int_biketrip_features"
)

SELECT
    station_id,
    date,
    hour,
    COUNT(*) AS num_bikes_taken
FROM base
GROUP BY 1,2,3
  );
  