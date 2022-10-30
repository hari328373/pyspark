# wide transformation

from pyspark import SparkContext
sc = SparkContext.getOrCreate()

# groupbykey
rdd_1 = sc.parallelize([(1,2),(1,3),(1,5),(2,5),(3,5),(5,8),(9,5)])

rdd_2 = rdd_1.groupByKey()
print([(j[0],[i for i in j[1]]) for j in rdd_2.collect()])

'''rdd_3 = rdd_1.groupByKey()
for (k,v) in rdd_3.collect():
    print((k,list(v)))'''