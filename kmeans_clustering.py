from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

spark = SparkSession.builder.appName("CustomerSegmentation").getOrCreate()

data = spark.createDataFrame([
    (1, "Raj", 250),
    (2, "Kumar", 1200),
    (3, "Lakshmi", 600),
    (4, "Priya", 700),
    (5, "Arun", 300),
    (6, "Meena", 1500),
    (7, "Ganesh", 450),
    (8, "Vani", 800)
], ["customer_id", "name", "total_spend"])

assembler = VectorAssembler(inputCols=["total_spend"], outputCol="features")
data = assembler.transform(data)

kmeans = KMeans().setK(3).setSeed(1)
model = kmeans.fit(data)

clusters = model.transform(data)

clusters.select("customer_id", "name", "total_spend", "prediction").show()
