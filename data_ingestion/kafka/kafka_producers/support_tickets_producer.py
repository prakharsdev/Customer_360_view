from kafka import KafkaProducer
import json
import time

def produce_support_tickets_data():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    support_tickets_data = [
        {"ticket_id": 1, "customer_id": 1, "issue": "Login problem", "timestamp": "2023-06-01T13:00:00Z"},
        {"ticket_id": 2, "customer_id": 2, "issue": "Payment issue", "timestamp": "2023-06-02T15:00:00Z"},
        # Add more sample data
    ]

    for data in support_tickets_data:
        producer.send('support_tickets_topic', value=data)
        print(f'Produced: {data}')
        time.sleep(1)  # Simulate delay

    producer.flush()
    producer.close()

if __name__ == "__main__":
    produce_support_tickets_data()
