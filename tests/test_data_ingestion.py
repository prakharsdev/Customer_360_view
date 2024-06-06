import unittest
from kafka import KafkaProducer, KafkaConsumer
import json

class TestDataIngestion(unittest.TestCase):

    def test_kafka_producer(self):
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        test_data = {"test": "data"}
        producer.send('test_topic', value=test_data)
        producer.flush()
        producer.close()

    def test_kafka_consumer(self):
        consumer = KafkaConsumer(
            'test_topic',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            group_id='test_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        for message in consumer:
            self.assertIsInstance(message.value, dict)
            break

if __name__ == '__main__':
    unittest.main()
