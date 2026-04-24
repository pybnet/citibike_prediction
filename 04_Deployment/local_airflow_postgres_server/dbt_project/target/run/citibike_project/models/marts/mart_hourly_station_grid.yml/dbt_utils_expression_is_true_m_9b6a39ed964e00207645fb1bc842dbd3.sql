
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

with meet_condition as (
    select * from "citibike_db"."public"."mart_hourly_station_grid" where 1=1
)

select
    *
from meet_condition

where not(day = extract(day from date_hour))


  
  
      
    ) dbt_internal_test