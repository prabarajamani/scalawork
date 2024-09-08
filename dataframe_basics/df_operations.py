import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, when
# df2 = df.select(col("EmpId"),col("Salary"),lit("1").alias("lit_value1"))
# df2.show(truncate=False)

from pyspark.sql.functions import when


os.environ['JAVA_HOME'] = "C:\\Program Files\\JetBrains\\IntelliJ IDEA Community Edition 2021.3.2\\jbr"
sales1 = "..\\dw_dataset\\sales_1.csv"
sales2 = "..\\dw_dataset\\sales_2.csv"
sales3 = "..\\dw_dataset\\sales_3.csv"
product_meta = "..\\dw_dataset\\product_meta.csv"
spark = SparkSession.builder.appName("rdd_one").master("local[*]").getOrCreate()


def read_csv(filepath: str):
    raw_df = spark.read.option("header", "true").option("inferSchema", "true").csv(filepath)
    return raw_df


def add_store_id(df: DataFrame, store_id: int):
    df_store_id = df.withColumn("store_id", lit(store_id))
    return df_store_id


def add_discount_column(df: DataFrame):
    df_discount = df.withColumn("discount", df['item_qty'] * df['unit_price'] - df['total_amount'])
    return df_discount


def product_sales(sales_df: DataFrame, product_df: DataFrame):
    joined_df = product_df.join(sales_df, ["item_id"], "left_outer")
    return joined_df
    # sales_df.join(product_df, sales_df["item_id"] == product_df["item_id"], "left_outer")


# df = read_csv(sales1)
# df = add_store_id(df, 1)
# df_sales = add_discount_column(df)
# df_product = read_csv(product_meta)
# df_product_sales = product_sales(df_sales, df_product)
#
# df_product_sales = df_product_sales.groupBy("item_id").agg({'discount': 'sum',
#                                                             'total_amount': 'sum', 'item_id': 'count'})
# df_product_sales.show()
#
# df.show()



