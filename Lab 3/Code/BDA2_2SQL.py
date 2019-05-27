from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext, Row  
from pyspark.sql import functions as F
sc =SparkContext()
spark = SparkSession(sc)
sqlContext=SQLContext(sc)

temp_csv = spark.read.csv("/user/x_phiho/data/temperature-readings.csv", header=False,sep=";")
df=temp_csv.selectExpr("_c0 as Station_number", "_c1 as Date", "_c2 as Time", "_c3 as Temperature","_c4 as Quality")
df=df.withColumn('Date',F.to_date("Date", "yyyy-MM-dd"))
df.createOrReplaceTempView("temps_table")
query_over10=" SELECT YEAR(Date) AS year , MONTH(Date) AS month ,COUNT(Temperature) AS count \
FROM temps_table WHERE FLOAT(Temperature) >=10 AND YEAR(Date) >=1950 AND YEAR(Date) <= 2014  GROUP BY year,month ORDER BY year,month ASC"

query_over10_rdd = query_over10.rdd

query_over10_rdd.saveAsTextFile("results/BDA2_2_SQL_results")
