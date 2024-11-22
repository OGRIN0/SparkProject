from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc


spark = SparkSession.builder.appName("ReadParquetExample").master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()

file_path = "data/laptop_price.csv"
df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .load(file_path)

threshold = 1000

high_value_laptops = df.filter(col("Price_euros")>threshold)\
    .select("Company", "Product", "Price_euros")

high_value_laptops.show(10)

top_selling_laptops = (
    df.groupby("Product", "Company")
    .agg(count("*").alias("Total_Sales"))
    .orderBy(desc("Total_Sales"))
)

top_selling_laptops.show(10)



