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

maxTempsYear=schemaTempReadings.select(["year","Temp"]).filter((schemaTempReadings['year'] <=2014) & (schemaTempReadings["year"]>=1950 ))\
.groupby(['year']).agg(F.max('Temp').alias("max_temp")).orderBy('max_temp',ascending=False)

minTempsYear=schemaTempReadings.select(["year","Temp"]).filter((schemaTempReadings['year'] <=2014) & (schemaTempReadings["year"]>=1950 ))\
.groupby(['year']).agg(F.min('Temp').alias("min_temp")).orderBy('min_temp',ascending=True)

maxTempsYear = maxTempsYear.rdd
minTempsYear = minTempsYear.rdd


maxTempsYear.saveAsTextFile("results/BDA2_1_resutls_max")
minTempsYear.saveAsTextFile("results/BDA2_1_resutls_max")