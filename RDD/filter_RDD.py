
from pyspark import SparkContext
sc=SparkContext.getOrCreate()

def statswith(w):
    return w.startswith("s")

#filter--->transformation
my_coll ="string is a data structure in python".split(' ')
words = sc.parallelize(my_coll)
filterwords = words.filter(lambda w:statswith(w))
print(filterwords.collect())

# filter
x_rdd = sc.parallelize([1,2,3,4,5])
y_rdd= x_rdd.filter(lambda x:x%2==0)
print(x_rdd.collect())
print(y_rdd.collect())