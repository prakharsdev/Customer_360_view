from pyspark.sql import SparkSession

def get_spark_session(app_name="Customer360"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
        .getOrCreate()
