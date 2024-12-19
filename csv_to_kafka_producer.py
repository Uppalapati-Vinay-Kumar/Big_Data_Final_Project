import time
import json
import csv
from kafka import KafkaProducer

# Function to read data from CSV and send to Kafka
def read_csv_and_produce(filename, producer, topic_name):
    """
    Reads a CSV file and sends each row as a message to a Kafka topic.

    Args:
        filename (str): Path to the CSV file.
        producer (KafkaProducer): Kafka producer instance.
        topic_name (str): Kafka topic name.

    Prints:
        - Total messages sent.
        - Time taken to send messages.
    """
    start_time = time.time()
    message_count = 0
    try:
        with open(filename, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)  
            for row in reader:
                message = json.dumps(row)
                message_count += 1
                producer.send(topic_name, message.encode())
    except Exception as e:
        print(f"Error reading CSV file: {e}")
    
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Total messages published: {message_count}")
    print(f"Total time taken to send all messages: {total_time:.2f} seconds")


# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    batch_size=327680,  
    linger_ms=50,       
    compression_type='gzip'  
)

csv_file = "spotify.csv"

try:
    read_csv_and_produce(csv_file, producer, "csvtopic")
except KeyboardInterrupt:
    print("Terminating the producer.")
finally:
    producer.close()
