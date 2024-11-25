from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, window

spark = SparkSession.builder \
    .appName("KafkaStockPriceProcessor") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
    .getOrCreate()


stock_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_prices") \
    .option("startingOffsets", "earliest")\
    .load()


stock_df = stock_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), "struct<stock_symbol:string, price:float, timestamp:long>").alias("parsed_data")) \
    .select("parsed_data.stock_symbol", "parsed_data.price", "parsed_data.timestamp")

stock_df = stock_df.withColumn("ts", from_unixtime(col("timestamp") / 1000).cast("timestamp")) \
    .drop("timestamp")

stock_df = stock_df.withWatermark("ts", "10 minutes")

windowed_avg_df = stock_df \
    .groupBy("stock_symbol", window("ts", "10 minutes")) \
    .agg({"price": "avg"}) \
    .select("stock_symbol", "window.start", "window.end", "avg(price)")

query = windowed_avg_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()
