
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        station_id, date_hour
    from "citibike_db"."public"."mart_hourly_station_grid"
    group by station_id, date_hour
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test