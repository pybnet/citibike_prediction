{{ config(
    materialized='table'
) }}

WITH pickups AS (
    SELECT
        station_id,
        date_trunc('hour', date_hour) AS date_hour,
        SUM(num_bikes_taken) AS num_bikes_taken
    FROM {{ ref('int_biketrip_hourly_pickups') }}
    GROUP BY station_id, date_hour
),

drops AS (
    SELECT
        station_id,
        date_trunc('hour', date_hour) AS date_hour,
        SUM(num_bikes_dropped) AS num_bikes_dropped
    FROM {{ ref('int_biketrip_hourly_drops') }}
    GROUP BY station_id, date_hour
)

SELECT
    COALESCE(p.station_id, d.station_id) AS station_id,
    COALESCE(p.date_hour, d.date_hour) AS date_hour,
    COALESCE(p.num_bikes_taken, 0) AS num_bikes_taken,
    COALESCE(d.num_bikes_dropped, 0) AS num_bikes_dropped,
    COALESCE(p.num_bikes_taken, 0) - COALESCE(d.num_bikes_dropped, 0) AS net_flow
FROM pickups p
FULL OUTER JOIN drops d
    ON p.station_id = d.station_id
    AND p.date_hour = d.date_hour
ORDER BY station_id, date_hour