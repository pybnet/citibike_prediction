{% test unique_date_hour(model) %}

    select
        station_id,
        date + hour * interval '1 hour' as date_hour,
        count(*) as row_count
    from {{ model }}
    group by 1, 2
    having count(*) > 1

{% endtest %}