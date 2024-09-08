import os
from pyspark.sql import SparkSession

os.environ['JAVA_HOME'] = "C:\\Program Files\\JetBrains\\IntelliJ IDEA Community Edition 2021.3.2\\jbr"

spark = SparkSession.builder.appName("rdd_one").master("local[*]").getOrCreate()


def print_rdd(rdd_to_print):
    output = rdd_to_print.collect()
    for l in output:
        print(l)


class Sales:
    def __init__(self, item_id: int, item_qty: int, unit_price: int, total_amount: int, date_of_sale: int):
        self.item_id = item_id
        self.item_qty = item_qty
        self.unit_price = unit_price
        self.total_amount = total_amount
        self.date_of_sale = date_of_sale

    def __str__(self):
        return f'Sales: {self.item_id}, {self.item_qty}, {self.unit_price}, {self.total_amount}, {self.date_of_sale}'


def split_function(line: str):
    arr = line.split(",")
    return (int(arr[0]), int(arr[1]), int(arr[2]), int(arr[3]), str(arr[4]))


def split_function_to_class(line: str):
    arr = line.split(",")
    # return (int(arr[0]), int(arr[1]), int(arr[2]), int(arr[3]), str(arr[4]))
    return Sales(item_id=int(arr[0]), item_qty=int(arr[1]), unit_price=int(arr[2]),
                 total_amount=int(arr[3]), date_of_sale=str(arr[4]))


def read_sales_file(filepath: str):
    rdd_lines = spark.sparkContext.textFile(filepath)
    rdd_sales = rdd_lines \
        .filter(lambda line: 'total_amount' not in line) \
        .map(lambda line: split_function_to_class(line))
    return rdd_sales


sales_file1 = "..\\datasets\\dw_dataset\\sales_1.csv"
sales_file2 = "..\\datasets\\dw_dataset\\sales_1.csv"

rdd_file1 = read_sales_file(sales_file1)
rdd_file2 = read_sales_file(sales_file2)
combined_rdd = rdd_file2.union(rdd_file1)
# print_rdd(combined_rdd)


class Product:
    def __init__(self, item_id, item_name, product_name, product_type):
        self.item_id = int(item_id)
        self.item_name = str(item_name)
        self.product_name = str(product_name)
        self.product_type = str(product_type)

    def __str__(self):
        return f'Product: {self.item_id}, {self.item_name}, {self.product_name}, {self.product_type}'

    @staticmethod
    def BuildFromLine(line: str):
        arr = line.split(',')
        return Product(arr[0], arr[1], arr[2], arr[3])


def read_product_file(filepath: str):
    rdd_lines = spark.sparkContext.textFile(filepath)
    rdd_product = rdd_lines \
        .filter(lambda line: 'item_name' not in line) \
        .map(lambda line: Product.BuildFromLine(line))
    return rdd_product


product_file = "..\\datasets\\dw_dataset\\product_meta.csv"
product_rdd = read_product_file(product_file)
# print_rdd(product_rdd)

prod_keyed_rdd = product_rdd.keyBy(lambda x: x.item_id)
#print_rdd(prod_keyed_rdd)

item_keyed_rdd = combined_rdd.keyBy(lambda x: x.item_id)

joined_rdd = item_keyed_rdd.join(prod_keyed_rdd)
# print_rdd(joined_rdd)

#exercise - remove the key

grouped_rdd = joined_rdd.groupBy(lambda x: x[1][1].product_type)
#print_rdd(grouped_rdd)

reduced_rdd = joined_rdd\
    .map(lambda x: (x[1][1].product_type, x[1][0].total_amount))\
    .reduceByKey(lambda total, tup: total + tup)
print_rdd(reduced_rdd)
#
#
# arr_reduced_rdd = reduced_rdd.collect()
# for l in arr_reduced_rdd:
#     print(l[0])
#     for k in l[1]:
#         print(k)

# print_rdd(reduced_rdd)