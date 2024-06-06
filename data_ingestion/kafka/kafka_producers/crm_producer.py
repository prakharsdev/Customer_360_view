from kafka import KafkaProducer
import json
import time

def produce_crm_data():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    crm_data = [
        {"customer_id": 1, "name": "John Doe", "email": "john.doe@example.com", "phone": "123-456-7890"},
        {"customer_id": 2, "name": "Jane Smith", "email": "jane.smith@example.com", "phone": "987-654-3210"},
        # Add more sample data
    ]

    for data in crm_data:
        producer.send('crm_topic', value=data)
        print(f'Produced: {data}')
        time.sleep(1)  # Simulate delay

    producer.flush()
    producer.close()

if __name__ == "__main__":
    produce_crm_data()
