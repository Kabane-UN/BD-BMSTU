import clickhouse_connect

client = clickhouse_connect.get_client(
    host="clickhouse", port=8123, username="default", password="password123"
)

with open("/tmp/last_loaded_dt.txt", "r") as f:
    last_loaded_dt = f.read().strip()

affected_days_query = f"""
SELECT DISTINCT
    toDate(tpep_pickup_datetime) AS day
FROM taxi.yellow_trips
WHERE tpep_pickup_datetime > '{last_loaded_dt}'
"""

affected_days = client.query(
    affected_days_query
).result_rows

affected_days = [row[0] for row in affected_days]

print("AFFECTED DAYS:", affected_days)


for day in affected_days:

    delete_query = f"""
    ALTER TABLE taxi.mart_avg_price_per_km
    DELETE WHERE day = '{day}'
    """

    client.command(delete_query)

days_sql = ",".join(
    [f"'{day}'" for day in affected_days]
)

rollback_row = f"""
ALTER TABLE taxi.yellow_trips
DELETE WHERE tpep_pickup_datetime > '{last_loaded_dt}'
"""
client.command(rollback_row)

rollback_mart = f"""
ALTER TABLE taxi.mart_avg_price_per_km
DELETE WHERE day IN ({days_sql})
"""
client.command(rollback_mart)

print("ROLLBACK FINISHED")
