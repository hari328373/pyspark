from pyspark import SparkContext,SparkConf

conf = SparkConf().setAppName("rdd").setMaster("local[*]")
sc = SparkContext(conf=conf)
inputwords = ["spark","hadoop","hive","pig","mahoot","sql"]
wordrdd = sc.parallelize(inputwords) # create RDD
words = wordrdd.collect()
print("wordsrdd :",words)
countrdd = wordrdd.count()
print("countrdd",countrdd)