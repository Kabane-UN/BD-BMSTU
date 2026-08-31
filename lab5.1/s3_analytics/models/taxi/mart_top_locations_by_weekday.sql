{{ 
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(weekday, rn)'
    ) 
}}

WITH ranked_locations AS
(
    SELECT
        weekday,

        PULocationID,

        revenue,

        row_number() OVER
        (
            PARTITION BY weekday
            ORDER BY revenue DESC
        ) AS rn

    FROM {{ ref('int_revenue_by_location') }}
)

SELECT
    weekday,

    PULocationID,

    revenue,

    rn

FROM ranked_locations

WHERE rn <= 10