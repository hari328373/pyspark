# transformation

from pyspark import SparkContext
sc=SparkContext.getOrCreate()
rdd1=sc.parallelize([1,2,3,4,5,6])
rdd_flatmap=rdd1.flatMap(lambda x : (x, x**2,100*x)) # return a sequence rather than a singled item
print("rdd1 : ",rdd1.collect())
print("flat_map :",rdd_flatmap.collect())