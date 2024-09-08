import os
from pyspark.sql import SparkSession, DataFrame

os.environ['JAVA_HOME'] = "C:\\Users\\Dell\\software\\jdk-17.0.12_7"
os.environ['HADOOP_HOME'] = "C:\\Users\\Dell\\software\\hadooputils\\bin\\winutils"
filepath = "C:\\Users\Dell\PycharmProjects\scalawork\datasets\dw_dataset\sales_1.csv"
amount_field_pos = 3

spark = SparkSession.builder.appName("rdd_one").master("local[*]").getOrCreate()
rdd_lines = spark.sparkContext.textFile(filepath)
rdd_amounts = rdd_lines\
    .filter(lambda line: str(line.split(',')[amount_field_pos]) != 'total_amount') \
    .map(lambda line: int(line.split(',')[amount_field_pos]))

lines = rdd_amounts.collect()
for l in lines:
    print(l)

total_amount = rdd_amounts.sum()
print(f'total_amount = {total_amount}')
