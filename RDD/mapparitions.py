# mapparitions


from pyspark import SparkContext
sc = SparkContext.getOrCreate()


x = sc.parallelize([1,1,2,3,4,5,6,7,8,9],2)
print("x:",x.collect())
print("x glom:",x.glom().collect()) # glom()--> flattens the elements on the same partition
def f(iter):yield sum(iter)
y = x.mapPartitions(f)
print("y:",y.collect())
print("y glom:",y.glom().collect())




# mappartitionwithindex

def f1(parindex,iter) : yield (parindex,sum(iter))
z = x.mapPartitionsWithIndex(f1)
print("z glom :",z.glom().collect())
