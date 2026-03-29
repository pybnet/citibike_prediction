
  
    

  create  table "citibike_db"."public"."stg_weather_hourly__dbt_tmp"
  
  
    as
  
  (
    

WITH base AS (
    SELECT
        date_trunc('hour', time) AS date_hour,
        temp,
        relative_humidity,
        precipitation_total,
        average_wind_speed,
        CASE 
            WHEN coco ~ '^\d+(\.\d+)?$' THEN CAST(coco AS numeric)::integer
            ELSE NULL
        END AS coco
    FROM "citibike_db"."public"."weather_data"
),

dedup AS (
    -- Deduplicate by date_hour and aggregate numeric columns
    SELECT
        date_hour,
        AVG(temp) AS temp,
        AVG(relative_humidity) AS relative_humidity,
        AVG(precipitation_total) AS precipitation_total,
        AVG(average_wind_speed) AS average_wind_speed,
        MAX(coco) AS coco
    FROM base
    GROUP BY date_hour
),

clean AS (
    -- Remove rows with any NULL values
    SELECT *
    FROM dedup
    WHERE temp IS NOT NULL
      AND relative_humidity IS NOT NULL
      AND precipitation_total IS NOT NULL
      AND average_wind_speed IS NOT NULL
      AND coco IS NOT NULL
)

SELECT
    *,
    CASE
        WHEN coco IN (1,2,3,4,5,6) THEN 'Pas de pluie'
        WHEN coco = 17 THEN 'Averse de pluie'
        WHEN coco IN (8,9,10,11,12,13,14,15,16,18,19,20,21,22,23,24,25,26,27) THEN 'Pluie/Neige'
        ELSE 'Autre'
END AS coco_group
FROM clean
ORDER BY date_hour
  );
  