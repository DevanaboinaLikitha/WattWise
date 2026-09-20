from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, BooleanType

KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "energy-readings"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/wattwise"
POSTGRES_PROPERTIES = {
    "user": "wattwise",
    "password": "changeme",
    "driver": "org.postgresql.Driver"
}

# SeaweedFS S3-compatible endpoint, using Docker's internal network
S3_ENDPOINT = "http://seaweedfs:8333"
S3_ACCESS_KEY = "wattwiseadmin"
S3_SECRET_KEY = "wattwisesecret"
RAW_DATA_PATH = "s3a://raw/energy-readings/"

LOCATION_THRESHOLDS = {
    "Room-101":    2.0,
    "Room-102":    2.0,
    "Lab-2":       6.0,
    "Library":     4.0,
    "Server-Room": 15.0,
}

reading_schema = StructType() \
    .add("location", StringType()) \
    .add("timestamp", StringType()) \
    .add("power_kw", DoubleType()) \
    .add("is_anomaly_injected", BooleanType())

spark = SparkSession.builder \
    .appName("WattWise-ThresholdAlerts") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), reading_schema).alias("data")
).select("data.*") \
 .withColumn("timestamp", to_timestamp(col("timestamp")))

# --- Write path 1: archive ALL raw readings to SeaweedFS as Parquet ---
raw_archive_query = parsed_stream.writeStream \
    .format("parquet") \
    .option("path", RAW_DATA_PATH) \
    .option("checkpointLocation", "/tmp/checkpoints/raw-archive") \
    .outputMode("append") \
    .start()

# --- Write path 2: threshold-based alerts to Postgres (unchanged logic) ---
threshold_expr = None
for loc, max_kw in LOCATION_THRESHOLDS.items():
    if threshold_expr is None:
        threshold_expr = when(col("location") == loc, lit(max_kw))
    else:
        threshold_expr = threshold_expr.when(col("location") == loc, lit(max_kw))

enriched_stream = parsed_stream.withColumn("threshold_kw", threshold_expr)
alerts_stream = enriched_stream.filter(col("power_kw") > col("threshold_kw"))

def write_alerts_to_postgres(batch_df, batch_id):
    count = batch_df.count()
    if count > 0:
        print(f"[Batch {batch_id}] Found {count} alert(s) - writing to Postgres.")
        batch_df.select("location", "timestamp", "power_kw", "threshold_kw") \
            .withColumnRenamed("timestamp", "reading_timestamp") \
            .write \
            .jdbc(url=POSTGRES_URL, table="energy_alerts", mode="append", properties=POSTGRES_PROPERTIES)
    else:
        print(f"[Batch {batch_id}] No alerts.")

alerts_query = alerts_stream.writeStream \
    .foreachBatch(write_alerts_to_postgres) \
    .outputMode("append") \
    .start()

# Wait for BOTH streaming queries to run indefinitely
spark.streams.awaitAnyTermination()
