
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select station_id
from "citibike_db"."public"."mart_hourly_station_grid"
where station_id is null



  
  
      
    ) dbt_internal_test