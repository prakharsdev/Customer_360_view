from kafka import KafkaProducer
import json
import time

def produce_transactions_data():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    transactions_data = [
        {"transaction_id": 1, "customer_id": 1, "amount": 100.0, "timestamp": "2023-06-01T12:00:00Z"},
        {"transaction_id": 2, "customer_id": 2, "amount": 200.0, "timestamp": "2023-06-02T14:00:00Z"},
        # Add more sample data
    ]

    for data in transactions_data:
        producer.send('transactions_topic', value=data)
        print(f'Produced: {data}')
        time.sleep(1)  # Simulate delay

    producer.flush()
    producer.close()

if __name__ == "__main__":
    produce_transactions_data()
