
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select net_flow_lag_2
from "citibike_db"."public"."mart_hourly_station_grid"
where net_flow_lag_2 is null



  
  
      
    ) dbt_internal_test