from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
spark = SparkSession.builder.master("local[*]").appName("df").getOrCreate()
sc=SparkContext.getOrCreate()
emptyRDD = sc.emptyRDD()
schema= StructType([
    StructField("firstname",StringType(),True),
    StructField("lastname",StringType(),True),
    StructField("id",StringType(),False),
    StructField("salary",IntegerType(),False)
])

dummy_df = spark.createDataFrame(sc.emptyRDD(),schema)
dummy_df.show()
dummy_df.printSchema()




