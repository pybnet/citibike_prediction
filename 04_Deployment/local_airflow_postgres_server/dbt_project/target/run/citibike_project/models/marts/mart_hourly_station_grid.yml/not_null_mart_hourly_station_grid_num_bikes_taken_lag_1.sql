
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select num_bikes_taken_lag_1
from "citibike_db"."public"."mart_hourly_station_grid"
where num_bikes_taken_lag_1 is null



  
  
      
    ) dbt_internal_test