from pyspark.sql import SparkSession

# Start a local Spark session (driver + executors on your machine)
spark = SparkSession.builder \
    .appName("TestSession") \
    .master("local[*]") \
    .getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

df.show()

spark.stop()
