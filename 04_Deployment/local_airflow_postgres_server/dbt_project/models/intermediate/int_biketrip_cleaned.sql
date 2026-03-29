{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM {{ ref('stg_biketrip_raw') }}
),

-- Cast to text
casted AS (
    SELECT
        *,
        start_station_id::text AS start_station_id_str,
        end_station_id::text AS end_station_id_str
    FROM base
),

-- Normalize format (add trailing 0 if needed)
normalized AS (
    SELECT
        *,
        CASE
            WHEN position('.' in start_station_id_str) > 0
             AND length(split_part(start_station_id_str, '.', 2)) = 1
            THEN split_part(start_station_id_str, '.', 1) || '.' ||
                 split_part(start_station_id_str, '.', 2) || '0'
            ELSE start_station_id_str
        END AS start_station_id_clean,

        CASE
            WHEN position('.' in end_station_id_str) > 0
             AND length(split_part(end_station_id_str, '.', 2)) = 1
            THEN split_part(end_station_id_str, '.', 1) || '.' ||
                 split_part(end_station_id_str, '.', 2) || '0'
            ELSE end_station_id_str
        END AS end_station_id_clean
    FROM casted
),

-- Keep only valid stations (JOIN instead of Python filtering)
filtered AS (
    SELECT n.*
    FROM normalized n
    WHERE EXISTS (
        SELECT 1
        FROM {{ ref('stg_citibike_stations') }} s
        WHERE n.start_station_id_clean = s.short_name
    )
    AND EXISTS (
        SELECT 1
        FROM {{ ref('stg_citibike_stations') }} s
        WHERE n.end_station_id_clean = s.short_name
    )
)

SELECT *
FROM filtered