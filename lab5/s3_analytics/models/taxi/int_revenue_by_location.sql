{{ 
    config(
        materialized='ephemeral'
    ) 
}}

SELECT
    toDayOfWeek(tpep_pickup_datetime) AS weekday,

    PULocationID,

    sum(total_amount) AS revenue

FROM {{ ref('taki_from_s3_extension') }}

GROUP BY
    weekday,
    PULocationID