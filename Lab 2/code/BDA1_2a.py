from pyspark import SparkContext
from pyspark.sql import SQLContext, Row  
from pyspark.sql import functions as F
sc =SparkContext()
sqlContext=SQLContext(sc)

T = sc.textFile("/user/x_phiho/data/temperature-readings.csv")

lines = T.map(lambda line: line.split(";"))


y = lines.map(lambda x: ( (x[1][0:4],x[1][5:7]),float(x[3]) ))

y = y.filter(lambda x: int(x[0][0]) >= 1950 and int(x[0][0]) <= 2014)

#############################
tempsOver= y.filter(lambda x : x[1] >= 10)

tempsOver_unpack=tempsOver.map(lambda x: ( (x[0][0], x[0][1]),1))

tempsOver10=tempsOver_unpack.reduceByKey(lambda x,y : x+y).sortByKey()


tempsOver10.saveAsTextFile("results/A2a")


