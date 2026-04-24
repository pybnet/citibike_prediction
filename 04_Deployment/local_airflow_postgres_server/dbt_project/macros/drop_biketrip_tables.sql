{% macro drop_biketrip_tables() %}

{% set tables = [
    "public.int_biketrip_cleaned",
    "public.int_biketrip_features",
    "public.int_biketrip_hourly_drops",
    "public.int_biketrip_hourly_pickups",
    "public.int_biketrip_hourly_net_flow",
    "public.mart_hourly_station_grid",
    "public.int_weather_hourly"
] %}

{% for table in tables %}
    {% do run_query("DROP TABLE IF EXISTS " ~ table ~ " CASCADE") %}
{% endfor %}

{% endmacro %}