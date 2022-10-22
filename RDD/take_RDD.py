from pyspark import SparkContext,SparkConf
conf = SparkConf().setAppName("take").setMaster("local[*]")
sc = SparkContext(conf=conf)
inputwords = ["spark","hadoop","hive","pig","mahoot","sql"]
word_rdd = sc.parallelize(inputwords)
words = word_rdd.take(3) # gives the required number of elements in RDD
print(words)