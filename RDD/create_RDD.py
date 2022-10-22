# create RDD

from pyspark import SparkContext,SparkConf


conf = SparkConf().setAppName("take").setMaster("local[*]") # configure the spark
sc = SparkContext(conf=conf)  # initialize the spark
inputwords = ["spark","hadoop","hive","pig","mahoot","sql"]
wordrdd = sc.parallelize(inputwords) # create RDD
words = wordrdd.collect() # collect() ---> gives the all values in RDD
print(words)


from pyspark import SparkContext
sc = SparkContext.getOrCreate()
data = (1,'ravi',101),(2,'sam',102),(3,'hari',103)
rdd = sc.parallelize(data)
print(rdd.collect())