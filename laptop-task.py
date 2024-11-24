from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

spark= SparkSession.builder.appName("task1").getOrCreate()

fp= "data\laptop_price.csv"

df= spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .load(fp)

thresh= 1250

hvl= df.filter(col("Price_euros")>thresh)\
    .select("Company", "Product", "Price_euros")

hvl.show(15)
//high value laptops from the laptop_price.csv - comment by me not chatgpt

tsl=(
    df.groupBy("Product", "Company")
    .agg(count("*").alias("Ts"))
    .orderBy(desc("Ts"))
) 

tsl.show(15)
//top selling laptops from laptop_price.csv - comment by me not chatgpt
