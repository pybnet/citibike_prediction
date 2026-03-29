{{ config(materialized='table') }}

WITH base AS (
    SELECT
        end_station_id AS station_id,
        date_trunc('hour', ended_at_date + ended_at_hour * interval '1 hour') AS date_hour
    FROM {{ ref('int_biketrip_features') }}
)

SELECT
    station_id,
    date_hour,
    COUNT(*) AS num_bikes_dropped
FROM base
GROUP BY station_id, date_hour
ORDER BY station_id, date_hour