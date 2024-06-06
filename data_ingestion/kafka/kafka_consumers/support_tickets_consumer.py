from kafka import KafkaConsumer
import json

def consume_support_tickets_data():
    consumer = KafkaConsumer(
        'support_tickets_topic',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='support_tickets_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        data = message.value
        print(f'Consumed: {data}')
        # Process the consumed data

if __name__ == "__main__":
    consume_support_tickets_data()
