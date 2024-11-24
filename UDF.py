from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("Hello").master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()


data_set = [
    (1, "Product A", 25.0),
    (2, "Product B", 75.0),
    (3, "Product C", 150.0),
    (4, "Product D", 55.0),
]
col = ["ProductID", "ProductName", "Price"]

df = spark.createDataFrame(data_set, col)

def classify_price(price):
    if price >= 100:
        return "high"
    elif 50 <= price < 100:
        return "medium"
    else:
        return "low"

classify_price_udf = udf(classify_price, StringType())

df = df.withColumn("PriceCategory", classify_price_udf(df["Price"]))

df.show()
