{{ config(materialized='table') }}

WITH date_hours AS (
    SELECT generate_series(
        (SELECT MIN(date_hour) FROM {{ ref('stg_weather_hourly') }}),
        (SELECT MAX(date_hour) FROM {{ ref('stg_weather_hourly') }}),
        interval '1 hour'
    ) AS date_hour
),

weather_filled AS (
    SELECT
        dh.date_hour,
        (
            SELECT temp
            FROM {{ ref('stg_weather_hourly') }} w
            WHERE w.date_hour <= dh.date_hour
            ORDER BY w.date_hour DESC
            LIMIT 1
        ) AS temp,
        (
            SELECT relative_humidity
            FROM {{ ref('stg_weather_hourly') }} w
            WHERE w.date_hour <= dh.date_hour
            ORDER BY w.date_hour DESC
            LIMIT 1
        ) AS relative_humidity,
        (
            SELECT precipitation_total
            FROM {{ ref('stg_weather_hourly') }} w
            WHERE w.date_hour <= dh.date_hour
            ORDER BY w.date_hour DESC
            LIMIT 1
        ) AS precipitation_total,
        (
            SELECT average_wind_speed
            FROM {{ ref('stg_weather_hourly') }} w
            WHERE w.date_hour <= dh.date_hour
            ORDER BY w.date_hour DESC
            LIMIT 1
        ) AS average_wind_speed,
        (
            SELECT coco
            FROM {{ ref('stg_weather_hourly') }} w
            WHERE w.date_hour <= dh.date_hour
            ORDER BY w.date_hour DESC
            LIMIT 1
        ) AS coco,
        (
            SELECT coco_group
            FROM {{ ref('stg_weather_hourly') }} w
            WHERE w.date_hour <= dh.date_hour
            ORDER BY w.date_hour DESC
            LIMIT 1
        ) AS coco_group

    FROM date_hours dh
)

SELECT *
FROM weather_filled
ORDER BY date_hour