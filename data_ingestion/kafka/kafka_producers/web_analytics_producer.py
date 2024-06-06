from kafka import KafkaProducer
import json
import time

def produce_web_analytics_data():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    web_analytics_data = [
        {"session_id": 1, "customer_id": 1, "page": "home", "timestamp": "2023-06-01T12:10:00Z"},
        {"session_id": 2, "customer_id": 2, "page": "product", "timestamp": "2023-06-02T14:20:00Z"},
        # Add more sample data
    ]

    for data in web_analytics_data:
        producer.send('web_analytics_topic', value=data)
        print(f'Produced: {data}')
        time.sleep(1)  # Simulate delay

    producer.flush()
    producer.close()

if __name__ == "__main__":
    produce_web_analytics_data()
