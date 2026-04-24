
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

    select
        station_id,
        date + hour * interval '1 hour' as date_hour,
        count(*) as row_count
    from "citibike_db"."public"."mart_hourly_station_grid"
    group by 1, 2
    having count(*) > 1


  
  
      
    ) dbt_internal_test