from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# create DataFrame
data = [("james",20000),("jack",35000),("johny",55000),("joseph",15000)]
df = spark.createDataFrame(data,("name","salary"))
df.show()

# convert DataFrame to RDD
rdd = df.rdd

print(rdd.collect())