from pyspark import SparkContext
from pyspark.sql import SQLContext, Row  
from pyspark.sql import functions as F
sc =SparkContext()
sqlContext=SQLContext(sc)

T = sc.textFile("/user/x_phiho/data/temperature-readings.csv")

lines = T.map(lambda line: line.split(";"))

year_temperature = lines.map(lambda x: (x[1][0:4], float(x[3])))

year_temperature = year_temperature.filter(lambda x: int(x[0]) >= 1950 and int(x[0]) <= 2014)

max_temperatures = year_temperature.reduceByKey(lambda x,y :x if x>=y else y)

max_temperaturesSorted = max_temperatures.sortBy(ascending=False,keyfunc=lambda k:k[1])

max_temps=sqlContext.createDataFrame(max_temperaturesSorted, schema=['Year', 'Temp'])

min_temperatures = year_temperature.reduceByKey(lambda x,y :x if x<=y else y)

min_temperaturesSorted = min_temperatures.sortBy(ascending=True,keyfunc=lambda k:k[1])


min_temperaturesSorted.saveAsTextFile("results/A1a_min")

max_temperaturesSorted.saveAsTextFile("results/A1a_max")