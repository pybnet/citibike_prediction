
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select day
from "citibike_db"."public"."mart_hourly_station_grid"
where day is null



  
  
      
    ) dbt_internal_test