# transformation

from pyspark import SparkContext
sc = SparkContext.getOrCreate()
rdd = sc.parallelize([1,2,6,5,3,5,8,9])
result = rdd.groupBy(lambda x:x%2).collect()
for (x,y) in result:
    print([(x,sorted(y))])

