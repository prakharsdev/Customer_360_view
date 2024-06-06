from kafka import KafkaConsumer
import json

def consume_transactions_data():
    consumer = KafkaConsumer(
        'transactions_topic',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='transactions_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        data = message.value
        print(f'Consumed: {data}')
        # Process the consumed data

if __name__ == "__main__":
    consume_transactions_data()
