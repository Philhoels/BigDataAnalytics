from pyspark import SparkContext
from pyspark.sql import SQLContext, Row  
from pyspark.sql import functions as F
sc =SparkContext()
sqlContext=SQLContext(sc)

path = "/user/x_andch/data/temperature-readings.csv"
# read the file
T=sc.textFile(path)

# Print the version of SparkContext
print("The version of Spark Context in the PySpark shell is", sc.version)

# Print the Python version of SparkContext
print("The Python version of Spark Context in the PySpark shell is", sc.pythonVer)

# Print the master of SparkContext
print("The master of Spark Context in the PySpark shell is", sc.master)

###

lines = T.map(lambda line: line.split(";"))

year_temperature = lines.map(lambda x: (x[1][0:4], float(x[3])))

year_temperature = year_temperature.filter(lambda x: int(x[0]) >= 1950 and int(x[0]) <= 2014)

max_temperatures= year_temperature.reduceByKey(lambda x,y :x if x>=y else y)

max_temperaturesSorted=max_temperatures.sortBy(ascending=False,keyfunc=lambda k:k[1])

max_temps=sqlContext.createDataFrame(max_temperaturesSorted, schema=['Year', 'Temp'])

min_temperatures= year_temperature.reduceByKey(lambda x,y :x if x<=y else y)

min_temperaturesSorted=min_temperatures.sortBy(ascending=False,keyfunc=lambda k:k[1])

min_temps=sqlContext.createDataFrame(min_temperaturesSorted, schema=['Year', 'Temp'])

max_temps.registerTempTable("maxReadingsTable")

min_temps.registerTempTable("minReadingsTable")

###
print("----Table with max temperatures per year---")
max_temps.show()
print("----Table with min temperatures per year---")
min_temps.show()

