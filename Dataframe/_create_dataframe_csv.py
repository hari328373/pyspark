from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("pyspark_2").getOrCreate()


# create a table
def table_1(spark):
    table1 = spark.read.option("header", True).csv(r"C:\Users\HARI\Downloads\Table1.csv", inferSchema=True)
    return table1

t1= table_1(spark)
t1.show(truncate=False)