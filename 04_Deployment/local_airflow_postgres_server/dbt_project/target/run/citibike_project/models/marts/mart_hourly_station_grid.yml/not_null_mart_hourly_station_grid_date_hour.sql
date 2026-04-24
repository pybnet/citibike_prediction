
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_hour
from "citibike_db"."public"."mart_hourly_station_grid"
where date_hour is null



  
  
      
    ) dbt_internal_test