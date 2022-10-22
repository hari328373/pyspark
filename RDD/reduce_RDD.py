from pyspark import SparkContext
sc = SparkContext.getOrCreate()

# reduce--->Action
print("sum of num :",sc.parallelize(range(1,21)).reduce(lambda x,y:x+y))

# reducebykey-->Transformation

rdd_1 = sc.parallelize([(1,2),(1,3),(1,5),(2,5),(3,5),(5,8),(9,5)])
rdd_4 = rdd_1.reduceByKey(lambda x,y : x+y).collect()
print(rdd_4)