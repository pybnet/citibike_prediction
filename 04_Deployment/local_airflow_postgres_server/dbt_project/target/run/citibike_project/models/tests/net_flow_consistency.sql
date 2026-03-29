
  
    

  create  table "citibike_db"."public"."net_flow_consistency__dbt_tmp"
  
  
    as
  
  (
    -- models/tests/net_flow_consistency.sql
-- Test: net_flow in mart matches int_biketrip_hourly_net_flow
WITH original AS (
    SELECT station_id, date_hour, num_bikes_taken, num_bikes_dropped, net_flow
    FROM "citibike_db"."public"."int_biketrip_hourly_net_flow"
    WHERE net_flow <> 0
),

mart AS (
    SELECT station_id, date_hour, num_bikes_taken, num_bikes_dropped, net_flow
    FROM "citibike_db"."public"."mart_hourly_station_grid"
    WHERE net_flow <> 0
),

missing_in_mart AS (
    SELECT o.*
    FROM original o
    LEFT JOIN mart m
           ON o.station_id = m.station_id
          AND o.date_hour = m.date_hour
          AND o.num_bikes_taken = m.num_bikes_taken
          AND o.num_bikes_dropped = m.num_bikes_dropped
          AND o.net_flow = m.net_flow
    WHERE m.station_id IS NULL
),

extra_in_mart AS (
    SELECT m.*
    FROM mart m
    LEFT JOIN original o
           ON o.station_id = m.station_id
          AND o.date_hour = m.date_hour
          AND o.num_bikes_taken = m.num_bikes_taken
          AND o.num_bikes_dropped = m.num_bikes_dropped
          AND o.net_flow = m.net_flow
    WHERE o.station_id IS NULL
)

SELECT *
FROM missing_in_mart
UNION ALL
SELECT *
FROM extra_in_mart
  );
  