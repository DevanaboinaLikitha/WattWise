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
).select("data.*")

threshold_expr = None
for loc, max_kw in LOCATION_THRESHOLDS.items():
    if threshold_expr is None:
        threshold_expr = when(col("location") == loc, lit(max_kw))
    else:
        threshold_expr = threshold_expr.when(col("location") == loc, lit(max_kw))

enriched_stream = parsed_stream.withColumn("threshold_kw", threshold_expr)
alerts_stream = enriched_stream.filter(col("power_kw") > col("threshold_kw"))

def write_alerts_to_postgres(batch_df, batch_id):
    """Called once per micro-batch. Writes any alert rows to Postgres."""
    count = batch_df.count()
    if count > 0:
        print(f"[Batch {batch_id}] Found {count} alert(s) - writing to Postgres.")
        batch_df.select(
            col("location"),
            to_timestamp(col("timestamp")).alias("reading_timestamp"),
            col("power_kw"),
            col("threshold_kw")
        ).write \
            .jdbc(url=POSTGRES_URL, table="energy_alerts", mode="append", properties=POSTGRES_PROPERTIES)
    else:
        print(f"[Batch {batch_id}] No alerts.")

query = alerts_stream.writeStream \
    .foreachBatch(write_alerts_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
