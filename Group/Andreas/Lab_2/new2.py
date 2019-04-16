path = "/user/x_andch/data/temperature-readings.csv"
# make set up
from pyspark import SparkContext, SparkConf
sc = SparkContext(appName= "Test")
# read the file
temp  = sc.textFile(path)
lines = temp.map(lambda line: line.split(";"))
# create a list with year and temperature
year_temperature = lines.map(lambda x: (x[1][0:4], float(x[3])))
year_temperature_timeinterval = year_temperature.filter(lambda x: int(x[0]) >= 1950 and int(x[0]) <= 2014)

# get the max temperature 
max_temperature = year_temperature_timeinterval.reduceByKey(lambda a,b: a if a>=b else b)
max_temperature_sort = max_temperature.sortBy(ascending = False, keyfunc = lambda k: k[1])

# get the min temperature
min_temperature = year_temperature_timeinterval.reduceByKey(lambda a,b: a if a<=b else b)
min_temperature_sort = min_temperature.sortBy(ascending = True, keyfunc = lambda k: k[1])

# print the first 10 - max
n = 10
print("This are the first {} of the max temperature: ".format(n))
max_temperature_sort.saveAsTextFile("test")

