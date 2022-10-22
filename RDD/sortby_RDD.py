from pyspark import SparkContext
sc=SparkContext.getOrCreate()

def statswith(w):
    return w.startswith("s")

my_coll ="string is a data structure in python".split(' ')
words = sc.parallelize(my_coll)

d=words.sortBy(lambda w : len(w)*-1) # descending
print("descending order :",d.collect())

a = words.sortBy(lambda w:len(w)) # ascending 
print("ascending order :",a.collect())