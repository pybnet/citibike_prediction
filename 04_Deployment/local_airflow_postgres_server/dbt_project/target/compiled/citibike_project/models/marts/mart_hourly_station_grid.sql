

WITH stations AS (
    SELECT DISTINCT station_id
    FROM "citibike_db"."public"."int_biketrip_hourly_net_flow_dedup"
),

date_hours AS (
    SELECT generate_series(
        (SELECT MIN(date_hour) FROM "citibike_db"."public"."int_biketrip_hourly_net_flow_dedup"),
        (SELECT MAX(date_hour) FROM "citibike_db"."public"."int_biketrip_hourly_net_flow_dedup"),
        interval '1 hour'
    ) AS date_hour
),

full_index AS (
    SELECT s.station_id, dh.date_hour
    FROM stations s
    CROSS JOIN date_hours dh
),

bike_grid AS (
    SELECT
        fi.station_id,
        fi.date_hour,
        EXTRACT(hour FROM fi.date_hour) AS hour,
        COALESCE(nb.num_bikes_taken, 0) AS num_bikes_taken,
        COALESCE(nb.num_bikes_dropped, 0) AS num_bikes_dropped,
        COALESCE(nb.net_flow, 0) AS net_flow
    FROM full_index fi
    LEFT JOIN "citibike_db"."public"."int_biketrip_hourly_net_flow_dedup" nb
        ON nb.station_id = fi.station_id
        AND nb.date_hour = fi.date_hour
),

merged AS (
    SELECT
        bg.station_id,
        bg.date_hour,
        bg.hour,
        bg.num_bikes_taken,
        bg.num_bikes_dropped,
        bg.net_flow,
        COALESCE(wh.temp, 0) AS temp,
        COALESCE(wh.relative_humidity, 0) AS relative_humidity,
        COALESCE(wh.precipitation_total, 0) AS precipitation_total,
        COALESCE(wh.average_wind_speed, 0) AS average_wind_speed,
        COALESCE(wh.coco::integer, 0) AS coco,
        COALESCE(wh.coco_group, 'Autre') AS coco_group
    FROM bike_grid bg
    LEFT JOIN "citibike_db"."public"."int_weather_hourly" wh
        ON wh.date_hour = bg.date_hour
),

features AS (
    SELECT
        m.*,
        -- Extract year, month, day
        EXTRACT(YEAR FROM m.date_hour)::int AS year,
        EXTRACT(MONTH FROM m.date_hour)::int AS month,
        EXTRACT(DAY FROM m.date_hour)::int AS day,
        -- Net flow lags
        LAG(net_flow, 1) OVER (PARTITION BY station_id ORDER BY date_hour) AS net_flow_lag_1,
        LAG(net_flow, 2) OVER (PARTITION BY station_id ORDER BY date_hour) AS net_flow_lag_2,
        LAG(net_flow, 24) OVER (PARTITION BY station_id ORDER BY date_hour) AS net_flow_lag_24,
        -- Rolling averages
        AVG(net_flow) OVER (PARTITION BY station_id ORDER BY date_hour ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS net_flow_roll_3,
        AVG(net_flow) OVER (PARTITION BY station_id ORDER BY date_hour ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING) AS net_flow_roll_24,
        -- Bike lags
        LAG(num_bikes_taken, 1) OVER (PARTITION BY station_id ORDER BY date_hour) AS num_bikes_taken_lag_1,
        LAG(num_bikes_dropped, 1) OVER (PARTITION BY station_id ORDER BY date_hour) AS num_bikes_dropped_lag_1,
        -- Day-of-week in French
        CASE EXTRACT(DOW FROM date_hour)
            WHEN 0 THEN 'Dimanche'
            WHEN 1 THEN 'Lundi'
            WHEN 2 THEN 'Mardi'
            WHEN 3 THEN 'Mercredi'
            WHEN 4 THEN 'Jeudi'
            WHEN 5 THEN 'Vendredi'
            WHEN 6 THEN 'Samedi'
        END AS jour_semaine,
        -- Holiday flag
        COALESCE(dh.is_holiday, FALSE) AS is_holiday
    FROM merged m
    LEFT JOIN "citibike_db"."public"."dim_holidays" dh
        ON date_trunc('day', m.date_hour)::date = dh.date
)

SELECT
    station_id,
    date_hour,
    year,
    month,
    day,
    hour,
    temp,
    precipitation_total,
    relative_humidity,
    average_wind_speed,
    num_bikes_taken_lag_1,
    num_bikes_dropped_lag_1,
    net_flow_lag_1,
    net_flow_lag_2,
    net_flow_lag_24,
    net_flow_roll_3,
    net_flow_roll_24,
    jour_semaine,
    coco_group,
    is_holiday,
    coco,
    net_flow,
    num_bikes_taken,
    num_bikes_dropped
FROM features
ORDER BY station_id, date_hour