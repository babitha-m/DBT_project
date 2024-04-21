from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, max, min
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Create SparkSession
spark = SparkSession.builder \
    .appName("KafkaStreamProcessing_B") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .getOrCreate()

# Define schema for the incoming JSON data
schema = StructType([
    StructField("city", StringType(), True),
    StructField("co", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("lpg", FloatType(), True),
    StructField("smoke", FloatType(), True),
    StructField("temp", FloatType(), True)
])

# Kafka broker address
kafka_bootstrap_servers = 'localhost:9092'

# Subscribe to Kafka topic for city B
topic = 'Bengaluru'

# Read data from Kafka topic as DataFrame
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic) \
    .load()

# Convert value column from Kafka to JSON string
df = df.selectExpr("CAST(value AS STRING)")

# Parse JSON data and apply schema
df = df.select(from_json(df.value, schema).alias("data")).select("data.*")

# Perform aggregation on city B's data
aggregated_df = df.groupBy().agg(
    avg(col("temp")).alias("avg_temp"),
    max(col("temp")).alias("max_temp"),
    min(col("temp")).alias("min_temp"),
    avg(col("humidity")).alias("avg_humidity"),
    max(col("humidity")).alias("max_humidity"),
    min(col("humidity")).alias("min_humidity"),
    avg(col("lpg")).alias("avg_lpg"),
    max(col("lpg")).alias("max_lpg"),
    min(col("lpg")).alias("min_lpg"),
    avg(col("smoke")).alias("avg_smoke"),
    max(col("smoke")).alias("max_smoke"),
    min(col("smoke")).alias("min_smoke"),
    avg(col("co")).alias("avg_CO_conc"),
    max(col("co")).alias("max_CO_conc"),
    min(col("co")).alias("min_CO_conc")
)

# Write the aggregated data to console
query = aggregated_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()

# Stop SparkSession
#spark.stop()

