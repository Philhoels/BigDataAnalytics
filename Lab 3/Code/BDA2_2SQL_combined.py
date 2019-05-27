from pyspark import SparkContext
from pyspark.sql import SQLContext, Row  
from pyspark.sql import functions as F
sc =SparkContext()
sqlContext=SQLContext(sc)


temp=sc.textFile("/user/x_phiho/data/temperature-readings.csv")
parts=temp.map(lambda a: a.split(';'))
tempReadings=parts.map(lambda x: Row(station=x[0], date=x[1], year=int(x[1].split("-")[0]), month=int(x[1].split("-")[1]), time=x[2], value=float(x[3]), quality=x[4]))
schemaTempReadings=sqlContext.createDataFrame(tempReadings)

schemaTempReadings.registerTempTable("tempReadingsTable")

df_sql=sqlContext.sql('SELECT year, month, count(station) as value FROM tempReadingsTable WHERE year>=1950 and year<=2014 and value>10.0 group by year, month ORDER BY value DESC').show()

df_sql_distinct=sqlContext.sql('SELECT year, month, count(distinct station) as value FROM tempReadingsTable WHERE year>=1950 and year<=2014 and value>10.0 group by year, month ORDER BY value DESC').show()

df_sql=df_sql.rdd
df_sql_distinct=df_sql_distinct.rdd

df_sql.saveAsTextFile("results/BDA2_2SQL_results2")
df_sql_distinct.saveAsTextFile("results/BDA2_2SQL_distinct_results2")