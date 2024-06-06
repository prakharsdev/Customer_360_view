import unittest
from pyspark.sql import SparkSession

class TestDataStorage(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.appName("TestDataStorage").getOrCreate()

    def tearDown(self):
        self.spark.stop()

    def test_save_to_delta(self):
        df = self.spark.createDataFrame([
            {"customer_id": 1, "name": "John Doe"}
        ])
        df.write.format("delta").mode("overwrite").save("/tmp/test_delta")
        # Verify save
        df_loaded = self.spark.read.format("delta").load("/tmp/test_delta")
        self.assertEqual(df_loaded.count(), 1)

if __name__ == '__main__':
    unittest.main()
