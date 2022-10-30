from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# create RDD
rdd = spark.sparkContext.parallelize(
    [(1,1.0,"abc"),(2,2.0,"pqr"),(3,3.0,"xyz")])

'''r1=rdd.collect()
for i in r1:
    print(i)'''

# convert RDD to DataFrame

column = ["sno","float","string"]
'''df = rdd.toDF(schema=column)
df.show()
df.printSchema()'''

df1 = spark.createDataFrame(rdd,schema=column)
df1.show()
df1.printSchema()

