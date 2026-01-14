from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType

# 1. Initialize Spark Session with Mongo and Kafka dependencies
spark = SparkSession.builder \
    .appName("BlueskyEVAnalysis") \
    .config("spark.mongodb.write.connection.uri", "mongodb+srv://spark_user:bentaleb@cluster0.vqwdlym.mongodb.net/bluesky_db.ev_posts?appName=Cluster0") \
    .config("spark.mongodb.output.uri", "mongodb+srv://spark_user:bentaleb@cluster0.vqwdlym.mongodb.net/bluesky_db.ev_posts?appName=Cluster0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Define Schema (Must match the Producer payload)
schema = StructType([
    StructField("did", StringType(), True),
    StructField("time_us", LongType(), True),
    StructField("text", StringType(), True),
    StructField("lang", StringType(), True),
    StructField("created_at", StringType(), True)
])

# 3. Read Stream from Kafka
# Note: inside docker network, we use 'kafka:29092'
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "bluesky-posts") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Transform Data (Parse JSON)
json_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 5. Write Stream to MongoDB
# We use "append" mode to add new tweets as they arrive
query = json_df.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .option("forceDeleteTempCheckpointLocation", "true") \
    .outputMode("append") \
    .start()

print("Stream started... Listening for EV posts on Bluesky...")
query.awaitTermination()