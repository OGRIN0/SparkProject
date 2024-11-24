from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("Help").master("local[*]") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()

c_data= [
    (1, "Bigboy", "juggernut@g.com", "Tamilnadu"),
    (2, "Loadha", "lokitt@g.com", "Tamilnadu")
]

p_data= [
    (101, 1, "iphone", 80000, "2024-11-3"),
    (102, 2, "samsung", 50000, "2024-11-4")
]
c_schema=["c_id", "name", "email", "location"]
p_schema=["p_id", "c_id", "product", "amount", "date"]

customers= spark.createDataFrame(c_data, schema=c_schema)
purchases= spark.createDataFrame(p_data, schema=p_schema)

report = customers.join(purchases, on="c_id", how="left_outer") \
    .select("c_id", "name", "email", "location", "p_id", "product", "amount", "date") \
    .withColumn("report_generated", lit("2024-11-24"))

report.show(truncate=False)
