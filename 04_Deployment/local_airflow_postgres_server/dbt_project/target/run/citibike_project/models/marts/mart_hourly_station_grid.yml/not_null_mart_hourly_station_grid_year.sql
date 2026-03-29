
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select year
from "citibike_db"."public"."mart_hourly_station_grid"
where year is null



  
  
      
    ) dbt_internal_test