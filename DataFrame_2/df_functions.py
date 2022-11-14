from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("pyspark_2").getOrCreate()


# create a table
def table_1(spark):
    table1 = spark.read.option("header",True).csv(r"C:\Users\HARI\Downloads\Datafiles\Iris (1).csv",inferSchema=True)
    return table1

t1= table_1(spark)
t1.show(truncate=False)
t1.printSchema()
print("total no.of rows:",t1.count())

#where
t2=t1.where((t1.Species!="Iris-setosa")&(t1.Species!="Iris-versicolor"))
t2.show(truncate=False)
print(t2.count())

# limit
tl=t1.limit(5)
tl.show()

#substract
ts=t1.subtract(t2)
ts.show()
print("substract count:",ts.count())

#drop
ts.drop("Species").show()

#remove duplicates
t1.dropDuplicates(subset=["Species","SepalLengthCm","SepalWidthCm","PetalLengthCm","PetalWidthCm"]).show()
