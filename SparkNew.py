from sqlite3.dbapi2 import Timestamp

import  findspark
from pyspark.sql.types import TimestampType

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, from_unixtime, unix_timestamp
from pyspark.sql.types import TimestampType


spark = (SparkSession.builder.master("local[*]")
         .appName("Weather-analysis")
         .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
         .getOrCreate())
sc = spark.sparkContext

#initiating input and output
input_path = "input/weatherData.csv"
out_valid="output/valid"

#reading and processing
input_df=spark.read.csv("data/weatherData.csv")

valid_df = input_df.filter("'Temperature (c)' is not null and "
                           "'Humidity' is not null and"
                           "'Wind Speed' is not null and"
                           "'Pressure' is not null")


valid_df = valid_df.withColumnRenamed("_c0", "DateTime") \
    .withColumnRenamed("_c1", "Temperature") \
    .withColumnRenamed("_c2", "Humidity") \
    .withColumnRenamed("_c3", "Wind_Speed") \
    .withColumnRenamed("_c4", "Pressure")



valid_date_df = valid_df.withColumn("Date",
                                    to_date(unix_timestamp("DateTime", "MM/dd/yy hh:mm a")
                                            .cast("timestamp")))

valid_date_df.write.option("header","true")\
    .mode("overwrite")\
    .partitionBy("Date")\
    .csv(out_valid)


