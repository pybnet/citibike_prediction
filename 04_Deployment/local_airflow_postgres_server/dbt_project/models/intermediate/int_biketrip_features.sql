{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM {{ ref('int_biketrip_cleaned') }}
),

-- Compute duration
with_duration AS (
    SELECT *,
        EXTRACT(EPOCH FROM (ended_at - started_at))/60.0 AS duration_minutes
    FROM base
),

-- Apply hard filters (1–80 min)
duration_filtered AS (
    SELECT *
    FROM with_duration
    WHERE duration_minutes BETWEEN 1 AND 80
),

--Compute quantiles (1% and 99%)
duration_stats AS (
    SELECT
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY duration_minutes) AS q1,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_minutes) AS q99
    FROM duration_filtered
),

-- Apply quantile filtering
duration_cleaned AS (
    SELECT d.*
    FROM duration_filtered d
    CROSS JOIN duration_stats s
    WHERE d.duration_minutes BETWEEN s.q1 AND s.q99
),

--Haversine distance (accurate)
with_distance AS (
    SELECT *,
        6371 * 2 * ASIN(
            SQRT(
                POWER(SIN(RADIANS(end_lat - start_lat)/2), 2) +
                COS(RADIANS(start_lat)) *
                COS(RADIANS(end_lat)) *
                POWER(SIN(RADIANS(end_lng - start_lng)/2), 2)
            )
        ) AS distance_km
    FROM duration_cleaned
),

-- Round distance
distance_rounded AS (
    SELECT *,
        ROUND(distance_km::numeric, 2) AS distance_km_rounded
    FROM with_distance
),

-- Compute mean & stddev
distance_stats AS (
    SELECT
        AVG(distance_km_rounded) AS mu,
        STDDEV(distance_km_rounded) AS sigma
    FROM distance_rounded
),

-- Apply 3-sigma filter
final AS (
    SELECT d.*
    FROM distance_rounded d
    CROSS JOIN distance_stats s
    WHERE d.distance_km_rounded BETWEEN (s.mu - 3*s.sigma) AND (s.mu + 3*s.sigma)
),

-- Add time features
final_with_time AS (
    SELECT *,
        DATE_TRUNC('day', started_at) AS started_at_date,
        EXTRACT(hour FROM started_at)::int AS started_at_hour,
        DATE_TRUNC('day', ended_at) AS ended_at_date,
        EXTRACT(hour FROM ended_at)::int AS ended_at_hour
    FROM final
)

SELECT *
FROM final_with_time