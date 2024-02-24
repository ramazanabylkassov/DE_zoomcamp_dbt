{{ config(materialized='view') }}

WITH tripdata AS 
(
  SELECT *,
    row_number() OVER(PARTITION BY vendor_id, pickup_datetime) AS rn
  FROM {{ source('staging','yellow_taxi_data') }}
  WHERE vendor_id IS NOT NULL
)
SELECT 

-- identifiers
    {{ dbt_utils.generate_surrogate_key(['vendor_id', 'pickup_datetime']) }} AS tripid,
    {{ dbt.safe_cast("vendor_id", api.Column.translate_type("integer")) }} AS vendorid,
    {{ dbt.safe_cast("rate_code", api.Column.translate_type("integer")) }} AS ratecodeid,
    {{ dbt.safe_cast("pickup_location_id", api.Column.translate_type("integer")) }} AS pickup_locationid,
    {{ dbt.safe_cast("dropoff_location_id", api.Column.translate_type("integer")) }} AS dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    cast(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} AS passenger_count,
    cast(trip_distance AS NUMERIC) AS trip_distance,
    -- yellow cabs are always street-hail
    1 AS trip_type,

    -- payment info
    cast(fare_amount AS NUMERIC) AS fare_amount,
    cast(extra AS NUMERIC) AS extra,
    cast(mta_tax AS NUMERIC) AS mta_tax,
    cast(tip_amount AS NUMERIC) AS tip_amount,
    cast(tolls_amount AS NUMERIC) AS tolls_amount,
    cast(0 AS NUMERIC) AS ehail_fee,
    cast(imp_surcharge AS NUMERIC) AS improvement_surcharge,
    cast(total_amount AS NUMERIC) AS total_amount,
    coalesce({{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }},0) AS payment_type,
    {{ get_payment_type_description("payment_type") }} AS payment_type_description

FROM tripdata
WHERE rn = 1

-- dbt build -m <model.sql> --vars 'is_test_run: false'
{% if var('is_test_run', default=true) %}
    LIMIT 100
{% endif %}


