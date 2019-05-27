from pyspark import SparkContext
from pyspark.sql import SQLContext, Row  
from pyspark.sql import functions as F
sc =SparkContext()
sqlContext=SQLContext(sc)

temp=sc.textFile("/user/x_phiho/data/precipitation-readings.csv")

parts_temp=temp.map(lambda a: a.split(';'))
tempReadingsRow=parts_temp.map(lambda x: Row(station=x[0], date=x[1], year=int(x[1].split("-")[0]),month= int(x[1].split("-")[1]),time= x[2],Temp= float(x[3]),quality= x[4]))
schemaTempReadings=sqlContext.createDataFrame(tempReadingsRow)


precip = sc.textFile("/user/x_phiho/data/stations-Ostergotland.csv")

parts_precip=precip.map(lambda a: a.split(';'))
statOstRow=parts_precip.map(lambda x: Row(station=x[0], name=x[1]))
schemaStatOst=sqlContext.createDataFrame(statOstRow)

schemaTempReadings=schemaTempReadings.join(schemaStatOst, 'station', 'inner')

schemaTempReadings=schemaTempReadings.filter( (schemaTempReadings['year'] >= 1950) & (schemaTempReadings['year'] <= 2014) )

minMaxTemps=schemaTempReadings.groupby(['station', 'date', 'year', 'month']).agg(F.min('Temp'), F.max('Temp'))

avgMonthlyTemps=minMaxTemps.withColumn( 'dailyAvg', (minMaxTemps['min(Temp)']+minMaxTemps['max(Temp)'])/2.0 ).groupBy('year', 'month', 'station').agg(F.avg('dailyAvg').alias('avgMonthlyTempPerStat')).groupBy('year', 'month').agg(F.avg('avgMonthlyTempPerStat').alias('avgMonthlyTemperature'))

longTermAvgTemp=avgMonthlyTemps.filter(avgMonthlyTemps['year'] <= 1980).groupBy('month').agg(F.avg('avgMonthlyTemperature').alias('longTermAvgTemp'))

diff=avgMonthlyTemps.join(longTermAvgTemp, 'month', 'inner')
diff=diff.withColumn('difference', diff['avgMonthlyTemperature']-diff['longTermAvgTemp']).select('year', 'month', 'difference').orderBy(['year', 'month'], ascending=[0, 0]).show()

diff_rdd = diff.rdd

diff_rdd.saveAsTextFile("results/BDA2_6_results")