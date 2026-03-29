
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        is_holiday as value_field,
        count(*) as n_records

    from "citibike_db"."public"."mart_hourly_station_grid"
    group by is_holiday

)

select *
from all_values
where value_field not in (
    'True','False'
)



  
  
      
    ) dbt_internal_test