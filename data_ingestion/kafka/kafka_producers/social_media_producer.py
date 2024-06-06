from kafka import KafkaProducer
import json
import time

def produce_social_media_data():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    social_media_data = [
        {"post_id": 1, "customer_id": 1, "content": "Great product!", "timestamp": "2023-06-01T14:00:00Z"},
        {"post_id": 2, "customer_id": 2, "content": "Not satisfied with the service.", "timestamp": "2023-06-02T16:00:00Z"},
        # Add more sample data
    ]

    for data in social_media_data:
        producer.send('social_media_topic', value=data)
        print(f'Produced: {data}')
        time.sleep(1)  # Simulate delay

    producer.flush()
    producer.close()

if __name__ == "__main__":
    produce_social_media_data()
