from kafka import KafkaConsumer
import json

def consume_web_analytics_data():
    consumer = KafkaConsumer(
        'web_analytics_topic',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='web_analytics_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        data = message.value
        print(f'Consumed: {data}')
        # Process the consumed data

if __name__ == "__main__":
    consume_web_analytics_data()
