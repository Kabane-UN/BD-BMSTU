from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import clickhouse_connect

# ----------------------------
# Spark session
# ----------------------------

spark = (
    SparkSession.builder.appName("incremental_load")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        ",".join(
            [
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2",
                "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2",
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

# ----------------------------
# ClickHouse connection
# ----------------------------

client = clickhouse_connect.get_client(
    host="clickhouse", port=8123, username="default", password="password123"
)

# ----------------------------
# Get watermark
# ----------------------------

query = """
SELECT max(tpep_pickup_datetime)
FROM taxi.yellow_trips
"""

result = client.query(query).result_rows

last_loaded_dt = result[0][0]

with open("/tmp/last_loaded_dt.txt", "w") as f:
    f.write(str(last_loaded_dt))

print(f"LAST LOADED DATETIME: {last_loaded_dt}")

# ----------------------------
# Read Iceberg table
# ----------------------------

df = spark.table("demo.taxi.yellow_trips")

# ----------------------------
# Incremental filter
# ----------------------------

if last_loaded_dt is not None:

    df = df.filter(col("tpep_pickup_datetime") > last_loaded_dt)

# ----------------------------
# Count rows
# ----------------------------

rows_count = df.count()

print(f"NEW ROWS: {rows_count}")

if rows_count == 0:
    print("NO NEW DATA")
    spark.stop()
    exit(0)

# ----------------------------
# Write to ClickHouse
# ----------------------------

(
    df.write.format("jdbc")
    .option("url", "jdbc:clickhouse://clickhouse:8123/taxi")
    .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
    .option("dbtable", "yellow_trips")
    .option("user", "default")
    .option("password", "password123")
    .mode("append")
    .save()
)

print("INCREMENTAL LOAD FINISHED")

spark.stop()
