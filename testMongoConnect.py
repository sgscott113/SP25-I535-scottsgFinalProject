from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MongoDBConnectionTest") \
    .config("spark.mongodb.input.uri", "mongodb://<HOST>:<PORT>/<DB>.<COLLECTION>") \
    .getOrCreate()

try:
    df = spark.read.format("mongo").load()
    print("Connection successful!")
    df.show()
except Exception as e:
    print(f"Connection failed: {e}")
finally:
    spark.stop()
