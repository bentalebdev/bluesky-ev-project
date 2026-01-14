import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType

# 1. Securely Get Mongo URI from Environment Variable
# This prevents your password from being hardcoded in the file
mongo_uri = os.getenv("MONGO_URI")

if not mongo_uri:
    print("Error: MONGO_URI environment variable is not set.")
    sys.exit(1)

# 2. Initialize Spark Session with Mongo and Kafka dependencies
spark = SparkSession.builder \
    .appName("BlueskyEVAnalysis") \
    .config("spark.mongodb.write.connection.uri", mongo_uri) \
    .config("spark.mongodb.output.uri", mongo_uri) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 3. Define Schema (Must match the Producer payload)
schema = StructType([
    StructField("did", StringType(), True),
    StructField("time_us", LongType(), True),
    StructField("text", StringType(), True),
    StructField("lang", StringType(), True),
    StructField("created_at", StringType(), True)
])

# 4. Read Stream from Kafka
# Note: inside docker network, we use 'kafka:29092'
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "bluesky-posts") \
    .option("startingOffsets", "latest") \
    .load()

# 5. Transform Data (Parse JSON)
json_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 6. Write Stream to MongoDB
# We use "append" mode to add new tweets as they arrive
query = json_df.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .option("forceDeleteTempCheckpointLocation", "true") \
    .outputMode("append") \
    .start()

print("Stream started... Listening for EV posts on Bluesky...")
query.awaitTermination()