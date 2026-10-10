{{
    config(
        materialized='incremental'
    )
}}


{% set cols = ["vehicle_id", "driver_id", "trip_id", "customer_id", "trip_start_time", "trip_end_time", "fare_amount", "distance_km", "last_updated_timestamp"] %}

SELECT 
    {% for col in cols %}
        {{ col }}
            {% if not loop.last %}
                ,
            {% endif %}
    {% endfor %}
 FROM 
    {{ source("source_bronze", "trips")}}
{% if is_incremental() %}
WHERE 
    last_updated_timestamp > (SELECT coalesce(MAX(last_updated_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}
