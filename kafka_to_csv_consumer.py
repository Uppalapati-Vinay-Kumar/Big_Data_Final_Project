import csv
import json
from kafka import KafkaConsumer

def consume_and_save_to_csv(topic_name, bootstrap_servers, output_file, buffer_size=1000):
    """
    Consumes messages from a Kafka topic and saves them to a CSV file in batches.

    Args:
    - topic_name (str): Kafka topic name to consume from.
    - bootstrap_servers (str): Kafka broker addresses.
    - output_file (str): Path to the output CSV file.
    - buffer_size (int): Number of messages to buffer before writing to the CSV file.
    """
    message_count = 0  
    try:
        # Create a Kafka consumer
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            group_id='my-consumer-group',  
            auto_offset_reset='earliest',  
            enable_auto_commit=True,       
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))  
        )

        print(f"Connected to Kafka. Listening to topic: {topic_name}")
        
        # Open the CSV file for writing
        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
            writer = None
            buffer = [] 

            for message in consumer:
                data = message.value
                buffer.append(data)  
                message_count += 1  

                if writer is None:
                    fieldnames = list(data.keys())  
                    writer = csv.DictWriter(file, fieldnames=fieldnames)
                    writer.writeheader()  

                if len(buffer) >= buffer_size:
                    writer.writerows(buffer) 
                    print(f"Written {len(buffer)} messages to CSV.")
                    buffer.clear() 

                if message_count % 1000 == 0:
                    print(f"Received {message_count} messages so far.")

            if buffer:
                writer.writerows(buffer)
                print(f"Written final {len(buffer)} messages to CSV.")
    except Exception as e:
        print(f"Error consuming messages: {e}")
    finally:
        consumer.close()

    print(f"Total messages received: {message_count}")

topic = "csvtopic"
bootstrap_servers = "localhost:9092"
output_file = "received_spotify_data.csv"

try:
    consume_and_save_to_csv(topic, bootstrap_servers, output_file)
except KeyboardInterrupt:
    print("Consumer terminated.")
