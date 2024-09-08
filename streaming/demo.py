spark.conf.set("spark.sql.shuffle.partitions", 2)
from pyspark.sql.functions import split, explode

# lines = spark.readStream.text("file:///mnt/home/jgcslabtrainer2001/some_files")
# to read from local filesystem: run the shell as pyspark2 --master local[2]

lines = spark.readStream.text("text_files/")
words = lines.select(explode(split(lines.value, " ")).alias('word'))
wordCounts = words.groupBy("word").count()
query = wordCounts.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()

query.stop()
word_class_df = spark.read.option("header", "true").csv("word_class/file_article.txt")
word_class_df.show()


joined_df = words.join(word_class_df, ["word"])
wordClassCounts = joined_df.groupBy("word").count()
writer2 = wordClassCounts.writeStream.outputMode("complete").format("console")
query2 = writer2.start()

