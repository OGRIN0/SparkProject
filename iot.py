from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

spark = SparkSession.builder.appName("iotdata").master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()

file_path = "data/iot_sensor_data.json"


c_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("battery_level", IntegerType(), True),
    StructField("timestamp", TimestampType(), True)
])

df = spark.read.format("json").schema(c_schema).load(file_path)

df.printSchema()

