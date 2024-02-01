import csv
import time
from kafka import KafkaProducer
import pandas as pd
 

kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'sony'
 

csv_file_path = '/home/ubh01/Downloads/apple_quality.csv'
 

producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers,
                         value_serializer=lambda x: x.encode('utf-8'))
 
 data to Kafka
def send_to_kafka():
    with open(csv_file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            json_row = pd.Series(row).to_json()
            producer.send(kafka_topic, json_row)
            print("Sent data to Kafka:", json_row)
            time.sleep(5)  # Wait for 5 seconds before sending the next row
 
try:
    send_to_kafka()
except KeyboardInterrupt:
    pass
finally:
    producer.close()