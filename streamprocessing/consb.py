from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, min, max, col, count
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from kafka import KafkaConsumer
import json

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("CityBConsumer") \
    .getOrCreate()

# Define schema for JSON data
schema = StructType([
    StructField("city", StringType(), True),
    StructField("co", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("lpg", FloatType(), True),
    StructField("smoke", FloatType(), True),
    StructField("temp", FloatType(), True)
])

# Kafka broker address
bootstrap_servers = 'localhost:9092'

# Create Kafka consumer for city_b topic
consumer_b = KafkaConsumer('Bengaluru', bootstrap_servers=bootstrap_servers,
                           value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Consume messages from topic and process with Spark SQL
for message in consumer_b:
    data = message.value
    
    # Create DataFrame from JSON data
    df = spark.createDataFrame([data], schema=schema)
    
    # Perform aggregation using Spark SQL queries

    result = df.select(
        avg(col("co")).alias("avg_co"),
        avg(col("humidity")).alias("avg_humidity"),
        avg(col("lpg")).alias("avg_lpg"),
        avg(col("smoke")).alias("avg_smoke"),
        avg(col("temp")).alias("avg_temp"),
        min(col("co")).alias("min_co"),
        min(col("humidity")).alias("min_humidity"),
        min(col("lpg")).alias("min_lpg"),
        min(col("smoke")).alias("min_smoke"),
        min(col("temp")).alias("min_temp"),
        max(col("co")).alias("max_co"),
        max(col("humidity")).alias("max_humidity"),
        max(col("lpg")).alias("max_lpg"),
        max(col("smoke")).alias("max_smoke"),
        max(col("temp")).alias("max_temp")
    ).collect()[0]
    
    # Print aggregated data
    print(f"City B - Average CO: {result['avg_co']}, Average Humidity: {result['avg_humidity']}, Average LPG: {result['avg_lpg']}, Average Smoke: {result['avg_smoke']}, Average Temp: {result['avg_temp']}")
    print(f"City B - Min CO: {result['min_co']}, Min Humidity: {result['min_humidity']}, Min LPG: {result['min_lpg']}, Min Smoke: {result['min_smoke']}, Min Temp: {result['min_temp']}")
    print(f"City B - Max CO: {result['max_co']}, Max Humidity: {result['max_humidity']}, Max LPG: {result['max_lpg']}, Max Smoke: {result['max_smoke']}, Max Temp: {result['max_temp']}")




