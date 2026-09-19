from pyspark.sql import SparkSession

# Connect to our Spark cluster running in Docker
spark = SparkSession.builder \
    .appName("WattWise-ConnectionTest") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

print("Successfully connected to Spark cluster!")
print(f"Spark version: {spark.version}")

# Create a tiny test dataset and do something trivial with it
data = [("Room-101", 1.2), ("Lab-2", 4.5), ("Server-Room", 12.0)]
df = spark.createDataFrame(data, ["location", "power_kw"])

print("\nSample DataFrame:")
df.show()

spark.stop()
