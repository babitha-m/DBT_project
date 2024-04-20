from pyspark.sql import SparkSession
import time

# Create a SparkSession
spark = SparkSession.builder.appName("BatchProcessing").getOrCreate()

# Define the path to the CSV file
csv_path = "weatherHistory.csv"

# Define the time interval for batch processing in seconds
time_interval = 5

# Define the number of rows to process in each batch
batch_size = 24

# Load the CSV file into a DataFrame
df = spark.read.csv(csv_path, header=True, inferSchema=True)
df.createOrReplaceTempView("weather_data")

# Get the total number of rows in the DataFrame
total_rows = df.count()

# Calculate the number of batches
num_batches = total_rows // batch_size
if total_rows % batch_size != 0:
    num_batches += 1

# Process the data in batches
for i in range(num_batches):
    # Get the start and end indices for the current batch
    start = i * batch_size
    end = min((i + 1) * batch_size, total_rows)
    start = start + 2
    end = end + 2
    # Perform batch processing with SQL query
    sql_query = f"""
        SELECT 
            *
        FROM (
            SELECT 
                *, 
                ROW_NUMBER() OVER (ORDER BY `Formatted Date`) as row_num
            FROM weather_data
        ) 
        WHERE row_num BETWEEN {start} AND {end}
    """

    result = spark.sql(sql_query)
    result.show()

    time.sleep(time_interval)

# Stop the SparkSession
spark.stop()
