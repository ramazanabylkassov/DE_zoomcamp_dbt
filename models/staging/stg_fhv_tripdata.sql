{{ config(materialized='view') }}

SELECT 
    cast(dispatching_base_num AS NUMERIC) AS dispatching_base_num,
    cast(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    cast(dropOff_datetime AS TIMESTAMP) AS dropOff_datetime,
    cast(PUlocationID AS NUMERIC) AS PUlocationID,
    cast(DOlocationID AS NUMERIC) AS DOlocationID,
    cast(SR_Flag AS NUMERIC) AS SR_Flag,
    cast(Affiliated_base_number AS NUMERIC) AS Affiliated_base_number,

FROM {{ source('staging', 'fhv_taxi_data') }}
LIMIT 100