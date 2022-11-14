from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
spark = SparkSession.builder.master("local[*]").appName("df_union").getOrCreate()

data=[("james","all",102,20000),
      ("maria","anni",105,25400)]
schema=["firstname","lastname","id","salary"]
df1 = spark.createDataFrame(data=data,schema=schema)
print("dataframe-1")
df1.show(truncate=False)

data=[("Haritha","suma",328,25000),
      ("Aravind","moyyi",332,28000),
      ("maria","anni",105,25400)]
schema=["firstname","lastname","id","salary"]
df2 = spark.createDataFrame(data=data,schema=schema)
print("dataframe-2")
df2.show(truncate=False)

# union and unionall same in pyspark-  allowduplicates
print("union Dataframe")
uniondf= df1.union(df2)
uniondf.show(truncate=False)

# remove duplicates
print("remove duplicates in union df")
disDf = df1.union(df2).distinct()
disDf.show(truncate=False)

# intersection
print("intersection")
intersetDf= df1.intersect(df2)
intersetDf.show(truncate=False)