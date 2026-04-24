
    
    

with all_values as (

    select
        coco_group as value_field,
        count(*) as n_records

    from "citibike_db"."public"."mart_hourly_station_grid"
    group by coco_group

)

select *
from all_values
where value_field not in (
    'Pas de pluie','Averse de pluie','Pluie/Neige','Autre'
)


