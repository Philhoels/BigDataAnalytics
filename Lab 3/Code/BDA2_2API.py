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

tempsCounts=schemaTempReadings.select(["year","month","Temp"]).filter(schemaTempReadings.Temp>=10).\
groupby("year", "month").agg(F.count('Temp').alias("count"))\
.orderBy('year','month')

tempsCounts_rdd = tempsCounts.rdd

tempsCounts_rdd.saveAsTextFile("results/BDA2_2_API_results")
