from pyspark import SparkContext
sc = SparkContext.getOrCreate()


rdd_1 = sc.parallelize([(1,2),(2,3),(3,4),(4,5),(5,6)])
rdd_2 = sc.parallelize([(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')])

result = rdd_1.join(rdd_2)
print(result.collect())