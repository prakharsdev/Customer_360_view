from pyspark.sql import SparkSession

def save_to_delta(df, path):
    df.write.format("delta").mode("overwrite").save(path)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SaveToDelta").getOrCreate()

    # Example usage:
    customer_df = spark.read.json("path/to/customer_data.json")
    save_to_delta(customer_df, "/path/to/delta/customer_data")

    spark.stop()
