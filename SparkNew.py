import  findspark
findspark.init()

from pyspark.sql import SparkSession


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


valid_df.write.format('csv').mode('overwrite').option('header', True).save('out_valid')


