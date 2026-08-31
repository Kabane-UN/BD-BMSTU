{{ 
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='bucket'
    ) 
}}

WITH bucketed AS
(
    SELECT
        CASE
            WHEN duration_min < 5 THEN '0-5 min'
            WHEN duration_min < 15 THEN '5-15 min'
            WHEN duration_min < 30 THEN '15-30 min'
            WHEN duration_min < 60 THEN '30-60 min'
            ELSE '60+ min'
        END AS bucket,

        total_amount

    FROM {{ ref('int_trip_durations') }}
)

SELECT
    bucket,

    count() AS trips_count,

    median(total_amount) AS median_cost,

    round(
        count() * 100.0 /
        sum(count()) OVER (),
        2
    ) AS percent_of_total

FROM bucketed

GROUP BY bucket

ORDER BY trips_count DESC