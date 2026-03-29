{{ config(materialized='table') }}

WITH base AS (
    SELECT
        start_station_id AS station_id,
        date_trunc('hour', started_at_date + started_at_hour * interval '1 hour') AS date_hour
    FROM {{ ref('int_biketrip_features') }}
)

SELECT
    station_id,
    date_hour,
    COUNT(*) AS num_bikes_taken
FROM base
GROUP BY station_id, date_hour
ORDER BY station_id, date_hour