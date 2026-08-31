{{ 
    config(
        materialized='ephemeral'
    ) 
}}

SELECT
    dateDiff(
        'minute',
        tpep_pickup_datetime,
        tpep_dropoff_datetime
    ) AS duration_min,

    total_amount

FROM {{ ref('taki_from_s3_extension') }}