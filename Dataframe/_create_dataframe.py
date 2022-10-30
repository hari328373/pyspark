# create dataframe
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("demo").getOrCreate()
data = (1,'ravi',101),(2,'sam',102),(3,'hari',103)
columns = ['id','name','rollnum']
df = spark.createDataFrame(data=data,schema=columns)
df.show()


# textfile dataframe
df1=spark.read.text(r"C:\Users\HARI\OneDrive\Desktop\textfile.txt")
df1.show(truncate=False)
df1.printSchema()