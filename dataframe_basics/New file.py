import os
from pyspark.sql import SparkSession

from dataframe_basics.df_operations import product_meta

product_meta = "C:\Users\Dell\PycharmProjects\scalawork\datasets\dw_dataset\product_meta.csv"

spark = SparkSession.builder.appName("rdd_one").master("local[*]").getOrCreate()

df = spark.read.csv(product_meta)
df.show()