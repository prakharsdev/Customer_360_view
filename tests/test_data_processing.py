import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class TestDataProcessing(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.appName("TestDataProcessing").getOrCreate()

    def tearDown(self):
        self.spark.stop()

    def test_transformations(self):
        df = self.spark.createDataFrame([
            {"timestamp": "2023-06-01T12:00:00Z"}
        ])
        df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))
        self.assertEqual(df.schema["timestamp"].dataType.typeName(), "timestamp")

if __name__ == '__main__':
    unittest.main()
