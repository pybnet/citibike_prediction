
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_hour
from "citibike_db"."public"."int_weather_hourly"
where date_hour is null



  
  
      
    ) dbt_internal_test