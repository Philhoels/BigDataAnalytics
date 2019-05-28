from pyspark import SparkContext
from pyspark.sql import SQLContext, Row  
from pyspark.sql import functions as F
sc =SparkContext()
sqlContext=SQLContext(sc)

# with min max func
def min_max(v1, v2):
	outmin=v2[0] if v1[0] > v2[0] else v1[0]
	outmax=v1[1] if v1[1] > v2[1] else v2[1]
	return (outmin, outmax)

stationsOstg=sc.textFile("/user/x_phiho/data/stations-Ostergotland.csv")
stationsOstg=stationsOstg.map(lambda a: a.split(';')[0]).collect()

temperatures=sc.textFile("/user/x_phiho/data/temperature-readings.csv")
temperatures=temperatures.map(lambda a: a.split(';'))
temperatures=temperatures.filter(lambda x: int(x[1][0:4]) >= 1950 and int(x[1][0:4]) <= 2014)
temperatures=temperatures.filter(lambda x: x[0] in stationsOstg)


temperatures=temperatures.map(lambda x: (x[0]+';'+x[1], (float(x[3]), float(x[3]))))
temperatures=temperatures.reduceByKey(min_max)

temperatures=temperatures.map(lambda x: (x[0][:-3], (sum(x[1]), 2)))
temperatures=temperatures.reduceByKey(lambda v1, v2: (v1[0]+v2[0], v1[1]+v2[1]))
temperatures=temperatures.map(lambda x: (x[0], x[1][0]/x[1][1]))

temperatures=temperatures.map(lambda x: (x[0].split(';')[1], (x[1], 1)))
temperatures=temperatures.reduceByKey(lambda v1,v2: (v1[0]+v2[0], v1[1]+v2[1]))
temperatures=temperatures.map(lambda x: (x[0], x[1][0]/x[1][1]))

tempAvg=temperatures.filter(lambda x: int(x[0][:4]) <= 1980)
tempAvg=tempAvg.map(lambda x: (x[0][-2:], (x[1], 1)))
tempAvg=tempAvg.reduceByKey(lambda v1,v2: (v1[0]+v2[0], v1[1]+v2[1]))
tempAvg=tempAvg.map(lambda x: (int(x[0]), x[1][0]/x[1][1]))
tempAvg=tempAvg.collectAsMap()

differences=temperatures.map(lambda x: (x[0], x[1]-tempAvg[int(x[0][-2:])]))

differences.saveAsTextFile("results/A6")
tempAvg = tempAvg.map(lambda x: (int(x[0]), x[1][0]/x[1][1]))
tempAvg = tempAvg.collectAsMap()

# calculate the difference to the long term average for each month
differences = temperatures.map(lambda x: (x[0], x[1]-tempAvg[int(x[0][-2:])]))

# save the results
differences.saveAsTextFile("results/A6")
