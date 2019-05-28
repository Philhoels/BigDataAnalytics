from pyspark import SparkContext
from pyspark.sql import SQLContext, Row  
from pyspark.sql import functions as F
sc =SparkContext()
sqlContext=SQLContext(sc)

T = sc.textFile("/user/x_phiho/data/temperature-readings.csv")

lines = T.map(lambda line: line.split(";"))

####
rdd = lines.map(lambda x: ( (x[1][0:4],x[1][5:7],x[1][8:10],x[0]),float(x[3]) ))

rdd = rdd.filter(lambda x: int(x[0][0]) >= 1960 and int(x[0][0]) <= 2014)

#############################

rdd=rdd.map(lambda x: ( (x[0][0],x[0][1],x[0][2], x[0][3]),float(x[1])))

meanRDD = (rdd
           .mapValues(lambda x: (x, 1))
           .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
           .mapValues(lambda x: x[0]/x[1]))
meanRDD.sortByKey(ascending=False)

meanRDD.map(lambda x: ( (x[0][0],x[0][1],x[0][3]),x[1]) )\
.mapValues(lambda x: (x, 1))\
.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))\
.mapValues(lambda x: x[0]/x[1]).sortByKey(ascending=False)


meanRDD.saveAsTextFile("results/A3")

