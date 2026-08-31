from pyspark.sql import SparkSession
import subprocess
import clickhouse_connect

spark = (
    SparkSession.builder.appName("detect_snapshot")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        ",".join(
            [
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2",
            ]
        ),
    )
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.demo.type", "hadoop")
    .config("spark.sql.catalog.demo.warehouse", "s3a://warehouse/iceberg")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

snapshot_row = spark.sql(
    """ SELECT snapshot_id FROM demo.taxi.yellow_trips.snapshots ORDER BY committed_at DESC LIMIT 1 """
).collect()[0]
snapshot_id = snapshot_row["snapshot_id"]
print(f"LATEST SNAPSHOT: {snapshot_id}")
# check processed snapshot
client = clickhouse_connect.get_client(
    host="clickhouse", port=8123, username="default", password="password123"
)
query = f""" SELECT count() FROM taxi.processed_snapshots WHERE snapshot_id = {snapshot_id} """
count = client.query(query).result_rows[0][0]
if count > 0:
    print("SNAPSHOT_ALREADY_PROCESSED")
    exit(0)
print("NEW SNAPSHOT DETECTED")
# save snapshot_id for next tasks
with open("/tmp/current_snapshot.txt", "w") as f:
    f.write(str(snapshot_id))
print("SNAPSHOT SAVED")
spark.stop()
