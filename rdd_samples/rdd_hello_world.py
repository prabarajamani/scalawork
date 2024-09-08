import os
from pyspark.sql import SparkSession, DataFrame

os.environ['JAVA_HOME'] = "C:\\Program Files\\JetBrains\\IntelliJ IDEA Community Edition 2021.3.2\\jbr"
filepath = "..\\datasets\\dw_dataset\\sales_1.csv"
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
