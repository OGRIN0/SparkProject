from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum, rank

spark = SparkSession.builder.appName("window").master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()
data = [
    ("Arun Kumar", "Chennai", "2024-01", 15000, 2024),
    ("Lakshmi", "Coimbatore", "2024-01", 18000, 2024),
    ("Muthukumar", "Madurai", "2024-01", 20000, 2024),
    ("Arun Kumar", "Chennai", "2024-02", 17000, 2024),
    ("Lakshmi", "Coimbatore", "2024-02", 19000, 2024),
    ("Muthukumar", "Madurai", "2024-02", 22000, 2024),
    ("Arun Kumar", "Chennai", "2024-03", 16000, 2024),
    ("Lakshmi", "Coimbatore", "2024-03", 21000, 2024),
    ("Muthukumar", "Madurai", "2024-03", 23000, 2024),
]


columns = ["sales_rep", "region", "month", "sales", "year"]

df = spark.createDataFrame(data, columns)

cumulative_sales_window = Window.partitionBy("sales_rep").orderBy("month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df = df.withColumn("cumulative_sales", sum(col("sales")).over(cumulative_sales_window))

ranking_window = Window.partitionBy("month").orderBy(col("sales").desc())
df = df.withColumn("rank", rank().over(ranking_window))

df.show(truncate=False)
