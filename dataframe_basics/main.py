#from pyspark import SparkFiles
from pyspark.sql.connect.session import SparkSession

#from dataframe_adv.df_aggs import spark


def print_hi(name):
    print(f'Hi, {name}')


if __name__ == '__main__':
    print_hi('pycharm')

    spark = SparkSession.builder \
            .appName("Hello Spark") \
            .master("local[1]") \
            .getOrCreate()

    data_list =[("prem", 33),
                ("praba", 34),
                ("thasvin", 3)]

    df = spark.createDataFrame(data_list).toDF("Name", "Age")
    df.show()