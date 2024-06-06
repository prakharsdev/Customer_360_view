from kafka import KafkaConsumer
import json

def consume_customer_data():
    consumer = KafkaConsumer(
        'crm_topic',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='crm_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        data = message.value
        print(f'Consumed: {data}')
        # Process the consumed data

if __name__ == "__main__":
    consume_customer_data()
