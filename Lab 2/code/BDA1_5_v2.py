from pyspark import SparkContext
from pyspark.sql import SQLContext, Row  
from pyspark.sql import functions as F
sc =SparkContext()
sqlContext=SQLContext(sc)

path_A5stations = "/user/x_phiho/data/stations-Ostergotland.csv"
path_A5precipitation = "/user/x_phiho/data/precipitation-readings.csv"

ostergotland = sc.textFile(path_A5stations)
precipitation = sc.textFile(path_A5precipitation)

stations = ostergotland.map(lambda line: line.split(";")[0]).collect()

p_lines = precipitation.map(lambda line: line.split(";"))

valid_years = p_lines.filter(lambda x: int(x[1][0:4])>=1993 and int(x[1][0:4])<=2016)
valid_readings = valid_years.filter(lambda x: x[0] in stations)

####

formatted_readings = valid_readings.map(lambda x: ((x[1][0:7], x[0]), float(x[3])))


monthly_prec_per_station = formatted_readings.reduceByKey(lambda a,b: a+b)

average_monthly_prec = monthly_prec_per_station.map(lambda x: (x[0][0],  (x[1], 1)))

monthly_prec = average_monthly_prec.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1]))
monthly_prec = monthly_prec.map(lambda x: (x[0][0:4], x[0][5:7], x[1][0]/x[1][1]))

monthly_prec.saveAsTextFile("results/A5_v2")


