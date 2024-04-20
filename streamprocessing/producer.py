from kafka import KafkaProducer
import csv
import json
import time

# Kafka broker address
bootstrap_servers = 'localhost:9092'

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Function to read data from CSV file
def read_data_from_csv(csv_file):
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Convert relevant fields to float
            row['co'] = float(row['co'])
            row['humidity'] = float(row['humidity'])
            row['lpg'] = float(row['lpg'])
            row['smoke'] = float(row['smoke'])
            row['temp'] = float(row['temp'])
            yield row

# Path to the CSV file
csv_file = 'weather_city.csv'

# Continuously produce data to Kafka topics
for row in read_data_from_csv(csv_file):
    # Determine the topic based on the city column
    city = row['city']
    topic = city  # Create topic name dynamically based on city
    
    # Produce data to Kafka topic
    producer.send(topic, value=row)
    print(f"Produced to topic {topic}: {row}")
    
    # Wait for some time before producing next message (to simulate streaming data)
    time.sleep(5)  # 5 seconds interval

# Close Kafka producer
producer.close()


