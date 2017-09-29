import numpy
from scipy import spatial
from pyspark import SparkContext, SparkConf
import time

st = time.time() 

#建立 SparkConf 物件，這個物件包含應用程式資訊
conf = (SparkConf().setMaster("local[4]")
    .set("spark.executor.cores", "4")
    .set("spark.cores.max", "4")
    .set('spark.executor.memory', '20g')
)

#SparkContext為Spark的主要入口點,用於連接Spark集群、創建RDD、累加器（accumlator）、廣播變量（broadcast variables）
sc = SparkContext(conf=conf)

#將檔案從hdfs讀出
info = sc.textFile("hdfs://localhost/user/cloudera/cosine.csv")

#將餐廳資訊與tag切成 key-value 型式
infoTmp = info.map(lambda x: (x.split(",")[0].replace('"',''), x.split(",")[1:]))

#將資料做map
infoTmp = infoTmp.map(lambda x: (x[0], map(int, x[1])))

#對RDD進行笛卡兒計算
infoCombined = infoTmp.cartesian(infoTmp)

#使用scipy套件spatial作餘弦相似度計算
output = infoCombined.map(lambda x: x[0][0] + ";" + x[1][0] + ";" + str(1 - spatial.distance.cosine(x[0][1], x[1][1])))

#將檔案寫入hdfs之中
output.saveAsTextFile('hdfs://localhost/user/cloudera/test4/outputSimilarity')

et = time.time()

print et-st
