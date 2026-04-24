

SELECT
    station_id,
    date,
    SUM(num_bikes_taken) AS total_bikes_taken,
    SUM(num_bikes_dropped) AS total_bikes_dropped,
    SUM(net_flow) AS total_net_flow
FROM "citibike_db"."public"."int_biketrip_hourly_net_flow"
GROUP BY 1,2
ORDER BY station_id, date