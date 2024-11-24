from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

spark= SparkSession.builder.appName("task2").getOrCreate()

fp="data\iot_sensor_data.json"

fschema= StructType([
    StructField("sensor_id", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("battery_level", IntegerType(), True),
    StructField("timestamp", TimestampType(), True)
])

df= spark.read.format("json").schema(fschema).load(fp)

df.printSchema()
