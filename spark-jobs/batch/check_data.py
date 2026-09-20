from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("WattWise-DataCheck") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://seaweedfs:8333") \
    .config("spark.hadoop.fs.s3a.access.key", "wattwiseadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "wattwisesecret") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

df = spark.read.parquet("s3a://raw/energy-readings/")
print(f"\nTOTAL ROWS: {df.count()}")
df.groupBy("location").count().show()
spark.stop()
