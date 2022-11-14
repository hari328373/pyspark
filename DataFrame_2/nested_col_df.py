
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

Data = [
    (("james","","smith"),"36636",3100),
    (("michel","rose",""),"40288",4300),
    (("robert","","williams"),"42114",1400),
    (("maria","ame","jones"),"39192",1200),
    (("jen","marry","brown"),"",-1)
]

schema = StructType([
    StructField('Name',StructType([
        StructField('firstname',StringType(),True),
        StructField('middlename',StringType(),True),
        StructField('lastname',StringType(),True)
    ])),
    StructField('id',StringType(),True),
    StructField('salary',IntegerType(),True)
])

spark =  SparkSession.builder.master("local[*]").appName("struct").getOrCreate()
dataframe = spark.createDataFrame(data=Data,schema=schema)
dataframe.show(truncate=False)
dataframe.printSchema()

