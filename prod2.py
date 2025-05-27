from kafka import KafkaProducer
import json
import time
import pandas as pd

KAFKA_TOPIC = "traffic-data"
KAFKA_SERVER = "10.5.6.240:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

# Load dataset
filename = "traffic_data.csv"
df = pd.read_csv(filename)

while True:
    traffic_data = df.sample(n=20).to_dict(orient="records")  # Select 20 random rows
    producer.send(KAFKA_TOPIC, traffic_data)
    print(f"🚗 Sent {len(traffic_data)} rows of traffic data to Kafka.")
    time.sleep(5)  # Simulating new data every 5 seconds

