
    
    

with all_values as (

    select
        jour_semaine as value_field,
        count(*) as n_records

    from "citibike_db"."public"."mart_hourly_station_grid"
    group by jour_semaine

)

select *
from all_values
where value_field not in (
    'Dimanche','Lundi','Mardi','Mercredi','Jeudi','Vendredi','Samedi'
)


