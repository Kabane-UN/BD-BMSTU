{{ 
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='day'
    ) 
}}

SELECT
    assumeNotNull(toDate(tpep_pickup_datetime)) AS day,

    avg(
        total_amount / nullIf(trip_distance, 0)
    ) AS avg_price_per_km

FROM {{ ref('taki_from_s3_extension') }}

WHERE trip_distance > 0

GROUP BY day