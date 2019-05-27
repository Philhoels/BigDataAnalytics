from pyspark import SparkContext
from pyspark.sql import SQLContext, Row  
from pyspark.sql import functions as F
sc =SparkContext()
sqlContext=SQLContext(sc)

temps= sc.textFile("/user/x_phiho/data/temperature-readings.csv")
parts = temps.map(lambda l:l.split(";"))
tempReadings = parts.map(lambda p: Row(station=p[0],  date=p[1], year=p[1].split("-")[0],month=p[1].split("-")[1],day=p[1].split("-")[2], time=p[2],  Temp=float(p[3]), quality=p[4]))
#Inferring the schema and registering the DataFrame as atable
schemaTempReadings =  sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")


schemaAvg=schemaTempReadings.select('year','month','day','station','Temp')\
.filter((schemaTempReadings['year'] <=2014) & (schemaTempReadings["year"]>=1960 )).groupby( "year","month",'day','station')\
.agg( (F.avg('Temp') ).alias("daily_avg_temp"))\
.select('year','month','station','daily_avg_temp').groupBy('year','month','station')\
.agg( (F.avg('daily_avg_temp') ).alias("avg_temperature"))\
.orderBy('year','month','station',ascending=False)

schemaAvg_rdd = schemaAvg.rdd

schemaAvg_rdd.saveAsTextFile("results/BDA2_3_results")
