import clickhouse_connect

with open("/tmp/current_snapshot.txt") as f:
    snapshot_id = f.read().strip()

query = f"""
INSERT INTO taxi.processed_snapshots
(snapshot_id)
VALUES ({snapshot_id})
"""
client = clickhouse_connect.get_client(
    host="clickhouse", port=8123, username="admin", password="password123"
)

client.query(query)
print(f"SNAPSHOT {snapshot_id} MARKED AS PROCESSED")