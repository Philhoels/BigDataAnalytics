from pyspark import SparkContext
from pyspark.sql import SQLContext, Row  
from pyspark.sql import functions as F
sc =SparkContext()
sqlContext=SQLContext(sc)


temps= sc.textFile("/user/x_phiho/data/temperature-readings.csv")
parts = temps.map(lambda l:l.split(";"))
tempReadings = parts.map(lambda p: Row(station=p[0],  date=p[1], year=p[1].split("-")[0],month=p[1].split("-")[1], time=p[2],  Temp=float(p[3]), quality=p[4]))
#Inferring the schema and registering the DataFrame as atable
schemaTempReadings =  sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")

tempsDist=schemaTempReadings.select(["year","month","Temp"]).distinct().filter(schemaTempReadings.Temp>=10).\
groupby("year", "month").agg(F.count('Temp').alias("count"))\
.orderBy('year','month')


tempsDist_rdd = tempsDist.rdd 

tempsDist_rdd.saveAsTextFile("results/BDA2_2API_distinct_results")