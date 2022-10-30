
# wide Transformations

from pyspark import SparkContext
sc = SparkContext.getOrCreate()

rdd1 = sc.parallelize([(1,2),(1,3),(1,5),(2,5),(3,5),(5,8),(9,5),(2,9)])
rdd2 = rdd1.reduceByKey(lambda x,y:x+y)
print(rdd2.collect())