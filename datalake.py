from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract
from pyspark import SparkContext
import os




spark = SparkSession.builder \
    .appName("Process Log File with Delta Lake") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print(spark.version)



log_file_path = "/Users/amarnath/Documents/a3/newone/src/data_loader_import.txt"

if not os.path.exists(log_file_path):
    print(f"Log file does not exist at {log_file_path}")

log_rdd = spark.sparkContext.textFile(log_file_path)

print(log_rdd.take(5) )


pattern = r"(\d{2}/\d{2} \d{2}:\d{2}:\d{2})\s(\w+)\s+\: (.*)"


log_df = log_rdd.map(lambda line: (
    line[:17],
    line[18:22].strip(),
    "UNKNOWN",
    line[24:]
)).filter(lambda x: len(x[0]) > 0).toDF(["timestamp", "log_level", "module", "message"])

log_df.show(truncate=False)

output_path = "/Users/amarnath/Documents/a3/newone/code/clock"


log_df.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .save(output_path)




