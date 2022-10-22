import findspark
findspark.init()
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

data=([1,2,3,4,5])
rdd = sc.parallelize(data)
rdd_map=rdd.map(lambda x :(x,x**2)) # return the new RDD formed with given functionality
print("rdd : ",rdd.take(3))
print("rdd_map :",rdd_map.collect())