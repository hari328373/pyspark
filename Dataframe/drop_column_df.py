from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local").appName("demo").getOrCreate()
data = ('1','ravi','101'),('2','sam','102'),('3','hari','103')
columns = ['id','name','rollnum']
df = spark.createDataFrame(data=data,schema=columns)
df.show()

d2=df.drop("id") # delete the column
d2.show()