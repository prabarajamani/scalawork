from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name

from rdd_samples.rdd_hello_world import spark

spark = SparkSession.builder.appName("Simple").getOrCreate()

input_file = "D:\python\dataset\sales_1.csv"
df = spark.read.csv(input_file, header=True, inferSchema=True)

df.show()
spark.stop()