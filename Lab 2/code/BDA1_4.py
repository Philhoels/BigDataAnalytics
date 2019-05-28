from pyspark import SparkContext
from pyspark.sql import SQLContext, Row  
from pyspark.sql import functions as F
sc =SparkContext()
sqlContext=SQLContext(sc)

T = sc.textFile("/user/x_phiho/data/temperature-readings.csv")

l = T.map(lambda line: line.split(";"))

#####


station_temperature = l.map(lambda x:( (x[0],x[1]) ,float(x[3])))


max_temp_station= station_temperature.reduceByKey(lambda x,y :x if x>=y else y)

max_temp_station=max_temp_station.filter(lambda x: x[1]>=25. and x[1]<=30.)

max_temp_st=max_temp_station.map(lambda x: (x[0],float(x[1])))



prep = sc.textFile("/user/x_phiho/data/precipitation-readings.csv")

L=prep.map(lambda line: line.split(";"))

st = L.map(lambda x: ( (x[0],x[1]) ,float(x[3])) )

max_st= st.reduceByKey(lambda x,y :x if x>=y else y)

max_st=max_st.filter(lambda x: x[1]>=10. and x[1]<=20.)

wst=max_st.map(lambda x: (x[0],float(x[1])))

max_temp_st_result = max_temp_st.join(wst).collect()

max_temp_st_result.saveAsTextFile("results/A4")

