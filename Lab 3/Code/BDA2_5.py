from pyspark import SparkContext
from pyspark.sql import SQLContext, Row  
from pyspark.sql import functions as F
sc =SparkContext()
sqlContext=SQLContext(sc)


precipitation = sc.textFile("/user/x_phiho/data/precipitation-readings.csv")

parts=precipitation.map(lambda a: a.split(';'))
precReadingsRow=parts.map(lambda x: Row(station=x[0], date=x[1], year=int(x[1].split("-")[0]), month=int(x[1].split("-")[1]), time=x[2], precip=float(x[3]), quality=x[4]))
schemaPrecReadings=sqlContext.createDataFrame(precReadingsRow)

rdd = sc.textFile("/user/x_phiho/data/stations-Ostergotland.csv")

parts=rdd.map(lambda a: a.split(';'))
statOstRow=parts.map(lambda x: Row(station=x[0], name=x[1]))
schemaStatOst=sqlContext.createDataFrame(statOstRow)

schemaPrecReadings=schemaPrecReadings.filter( (schemaPrecReadings['year'] >= 1993) & (schemaPrecReadings['year'] <= 2016) )

schemaPrecReadings=schemaPrecReadings.join(schemaStatOst, 'station', 'inner')

schemaPrecReadings=schemaPrecReadings.groupBy('station', 'year', 'month').agg(F.sum('precip')).groupBy('year', 'month').agg(F.avg('sum(precip)').alias('avg_monthly_precipitation')).orderBy(['year', 'month'], ascending=[0, 0]).show()

schemaPrecReadings_rdd = schemaPrecReadings.rdd

schemaPrecReadings_rdd.saveAsTextFile("results/BDA2_5_results")