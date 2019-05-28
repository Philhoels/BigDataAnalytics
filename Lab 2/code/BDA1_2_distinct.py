from pyspark import SparkContext
from pyspark.sql import SQLContext, Row  
from pyspark.sql import functions as F
sc =SparkContext()
sqlContext=SQLContext(sc)

T = sc.textFile("/user/x_phiho/data/temperature-readings.csv")

lines = T.map(lambda line: line.split(";"))

y = lines.map(lambda x: (x[1][0:7], (x[0], float(x[3]))))

y = y.filter(lambda x: int(x[0][0:4])>=1950 and int(x[0][0:4])<=2014)
y = y.filter(lambda x: float(x[1][1]) >= 10)

month = y.map(lambda x: (x[0], x[1][0]))

month_unique = month.distinct()
unique_each_month = month_unique.map(lambda x: x[0])

month_count = unique_each_month.map(lambda s: (s,1))
counts = month_count.reduceByKey(lambda a,b: a+b)

counts.saveAsTextFile("results/BDA1_2_distinct_results")
