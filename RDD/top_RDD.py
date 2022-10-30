# Action

# top-->it returns the list sorted in descending order

from pyspark import SparkContext,SparkConf
conf = SparkConf().setAppName("top").setMaster("local[*]")
sc = SparkContext(conf=conf)
inputwords = [10,4,9,15,2,0,3,5,7]
word_rdd = sc.parallelize(inputwords)
print("top:",word_rdd.top(3))