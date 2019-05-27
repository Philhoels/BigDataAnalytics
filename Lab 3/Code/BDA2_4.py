from pyspark import SparkContext
from pyspark.sql import SQLContext, Row  
from pyspark.sql import functions as F
sc =SparkContext()
sqlContext=SQLContext(sc)

temps= sc.textFile("/user/x_phiho/data/temperature-readings.csv")
parts_temp = temps.map(lambda l:l.split(";"))
tempReadings = parts_temp.map(lambda p: Row(station=p[0],  date=p[1], year=p[1].split("-")[0],month=p[1].split("-")[1], time=p[2],  Temp=float(p[3]), quality=p[4]))
#Inferring the schema and registering the DataFrame as atable
schemaTempReadings =  sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")


precipitation = sc.textFile("/user/x_phiho/data/precipitation-readings.csv")

parts_precip=precipitation.map(lambda a: a.split(';'))
precReadingsRow=parts_precip.map(lambda x: Row(station=x[0], date=x[1], year=int(x[1].split("-")[0]), month=int(x[1].split("-")[1]), time=x[2], precip=float(x[3]), quality=x[4]))
precReadingsRow=sqlContext.createDataFrame(precReadingsRow)
precReadingsRow.registerTempTable("precipReadings")


sch=schemaTempReadings.groupby('station').agg(F.max('Temp').alias('maxTemp'))
sch1=sch.filter( (sch['maxTemp'] >25) & (sch['maxTemp'] <30) )

prec=precReadingsRow.groupby('station').agg(F.max('precip').alias('maxPrecip'))
prec1=prec.filter( (prec['maxPrecip'] >100) & (prec['maxPrecip'] <200) )

joinedSchema=sch1.join(prec1,'station')

joinedSchema_rdd = joinedSchema.rdd
joinedSchema_rdd.saveAsTextFile("results/BDA2_4_results")
