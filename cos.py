import numpy
from scipy import spatial
from pyspark import SparkContext, SparkConf
import time

st = time.time() 

conf = (SparkConf().setMaster("local[4]")
    .set("spark.executor.cores", "4")
    .set("spark.cores.max", "4")
    .set('spark.executor.memory', '20g')
)

sc = SparkContext(conf=conf)

l = sc.textFile("hdfs://localhost/user/cloudera/cosine1.csv")

lTmp = l.map(lambda x: (x.split(",")[0].replace('"',''), x.split(",")[1:]))

lTmp = lTmp.map(lambda x: (x[0], map(int, x[1])))

lCombined = lTmp.cartesian(lTmp)

output = lCombined.map(lambda x: x[0][0] + ";" + x[1][0] + ";" + str(1 - spatial.distance.cosine(x[0][1], x[1][1])))

output.saveAsTextFile('hdfs://localhost/user/cloudera/test4/outputSimilarity')

et = time.time()

print et-st
