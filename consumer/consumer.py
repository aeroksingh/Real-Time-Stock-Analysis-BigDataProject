import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

# ==============================
# 1. Create Spark Session
# ==============================
spark = SparkSession.builder \
    .appName("StockAnalysis") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==============================
# 2. Define Schema (VERY IMPORTANT)
# ==============================
schema = StructType() \
    .add("ticker", StringType()) \
    .add("date_time", StringType()) \
    .add("open", DoubleType()) \
    .add("high", DoubleType()) \
    .add("low", DoubleType()) \
    .add("close", DoubleType()) \
    .add("volume", LongType()) \
    .add("source", StringType())

# ==============================
# 3. Read from Kafka
# ==============================
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "stock-topic") \
    .option("startingOffsets", "latest") \
    .load()

# ==============================
# 4. Convert Kafka value to JSON
# ==============================
json_df = df.selectExpr("CAST(value AS STRING) as json")

parsed_df = json_df.select(
    from_json(col("json"), schema).alias("data")
).select("data.*")

# ==============================
# 5. Write to PostgreSQL
# ==============================
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/stock_analysis") \
        .option("dbtable", "stock_prices") \
        .option("user", "postgres") \
        .option("password", "stockpass123") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

print("🚀 Streaming started...")

query.awaitTermination()