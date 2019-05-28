from pyspark import SparkContext
from pyspark.sql import SQLContext, Row  
from pyspark.sql import functions as F
sc =SparkContext()
sqlContext=SQLContext(sc)

T = sc.textFile("/user/x_phiho/data/temperature-readings.csv")

lines = T.map(lambda line: line.split(";"))

# create a list with station & year and temperature
year_temperature_station = lines.map(lambda x: (x[1][0:4], (float(x[3]), x[0])))

# later on we use reduce by key - we have to create a key (year) by using a nested tuple
year_temperature_timeinterval_station = year_temperature_station.filter(lambda x: int(x[0]) >= 1950 and int(x[0]) <= 2014)

# get the max temperature including the station number
max_temperature_station = year_temperature_timeinterval_station.reduceByKey(lambda a,b: a if a>=b else b)
max_temperature_station_sort = max_temperature_station.sortBy(ascending = False, keyfunc = lambda k: k[1][0])

# get the min temperature including the station number
min_temperature_station = year_temperature_timeinterval_station.reduceByKey(lambda a,b: a if a<=b else b)
min_temperature_station_sort = min_temperature_station.sortBy(ascending = True, keyfunc = lambda k: k[1][0])


max_temperature_station.saveAsTextFile("results/A1a_min_part2")

max_temperature_station.saveAsTextFile("results/A1a_max_part2")







