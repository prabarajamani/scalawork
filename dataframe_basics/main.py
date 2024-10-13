import os
from pyspark.sql import SparkSession

def print_hi(name):
    print(f'Hi, {name}')

os.environ['JAVA_HOME'] = "D:\\software\\microsoft-jdk-17.0.12-windows-x64\\jdk-17.0.12+7\\"

product_data = "C:\\Users\\Thasvin Prabakaran\\PycharmProjects\\scalawork\\datasets\\dw_dataset\\product_meta.csv"
spark = SparkSession.builder.appName("Hello Spark").master("local[1]").getOrCreate()

df = spark.read.csv(product_data)
df.show()
