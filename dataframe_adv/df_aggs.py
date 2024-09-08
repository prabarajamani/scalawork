
# semi join
# window, partition by
# data partitions
import os
from pyspark.sql import SparkSession

from dataframe_basics.df_operations import sales1, sales2, sales3, product_meta, read_csv

os.environ['JAVA_HOME'] = "C:\\Program Files\\JetBrains\\IntelliJ IDEA Community Edition 2021.3.2\\jbr"


spark = SparkSession.builder.config("spark.executor.memory", "1g")\
    .config("spark.executor.instances", "2")\
    .appName("rdd_one").master("local[*]").getOrCreate()


def agg():
    sales1_df = read_csv(sales1)
    product_df = read_csv(product_meta)
    product_df
    rank_spec = Window