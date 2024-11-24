from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder.appName("Hello").master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()

customer_data = spark.createDataFrame([
    (1, "Iron Man", 45, "New York"),
    (2, "Captain America", 100, "Brooklyn"),
    (3, "Thor", 1500, "Asgard"),
    (4, "Black Widow", 35, "Moscow"),
    (5, "Hulk", 40, "New York"),
], ["customer_id", "name", "age", "location"])

purchase_data = spark.createDataFrame([
    (1, 200),
    (1, 150),
    (2, 300),
    (3, 500),
    (3, 200),
    (4, 100),
    (5, 400),
    (5, 300),
], ["customer_id", "purchase_amount"])

total_spend = purchase_data.groupBy("customer_id").agg(_sum("purchase_amount").alias("total_spend"))

data = customer_data.join(total_spend, on="customer_id", how="left")

assembler = VectorAssembler(inputCols=["total_spend"], outputCol="features")
data = assembler.transform(data)

data.select("customer_id", "name", "total_spend", "features").show()
