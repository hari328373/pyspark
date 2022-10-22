from pyspark import SparkContext
sc=SparkContext.getOrCreate()

# getNumPartitions
par = sc.parallelize([1,2,3,4,5,8,7,6,9,4,5,7,8],4)
print("num of partitions  in rdd ", par.getNumPartitions())

my_coll ="string is a data structure in python".split(' ')
words = sc.parallelize(my_coll)
print("num of words:",words.getNumPartitions())
print("words rdd :",words.collect())