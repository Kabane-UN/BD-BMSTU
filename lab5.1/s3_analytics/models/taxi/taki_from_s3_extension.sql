{{
    config(
        materialized='incremental',
        engine='MergeTree()',
        order_by='tpep_pickup_datetime'
    )
}}

SELECT 

	CAST(
					tpep_pickup_datetime AS DateTime
			) AS tpep_pickup_datetime,

			*

EXCEPT(tpep_pickup_datetime)

FROM iceberg(
    'http://minio:9000/warehouse/iceberg/taxi/yellow_trips',
    'admin',
    'password123'
)

WHERE tpep_pickup_datetime IS NOT NULL

{% if is_incremental() %}

AND tpep_pickup_datetime >
(
    SELECT max(tpep_pickup_datetime)
    FROM {{ this }}
)

{% endif %}