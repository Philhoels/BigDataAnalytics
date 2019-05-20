from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp 
from datetime import datetime
from pyspark import SparkContext
sc = SparkContext(appName="lab_kernel")
import os

def haversine(lon1, lat1, lon2, lat2):
    """Calculate the great circle distance between two points on the earth (specified in decimal degrees)"""
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2]) 
    # haversine formula
    dlon = lon2 -lon1 
    dlat = lat2 -lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2 
    c = 2 * asin(sqrt(a))
    km = 6367 * c 
    return km

# init values
h_distance = 100 # Up to you 
h_date  = 3 #  Up  to  you 
h_time = 2 # Up to you

a = 58.4274 # Up to you 
b = 14.826 # Up to you

times=["04:00:00", "06:00:00","08:00:00" ,"10:00:00","12:00:00","14:00:00",
       "16:00:00","18:00:00","20:00:00","22:00:00","00:00:00"]

date = "2013-07-04" # Up to you
CURRENT_DIR= "/user/x_andch/data/"

stations = sc.textFile(CURRENT_DIR+"stations.csv") 
temps = sc.textFile(CURRENT_DIR+"temperature-readings.csv")

# Your code here

def filter_dateRdd(target_day,rdd):
    targetDay=datetime.strptime(target_day,"%Y-%m-%d")
    rdd=rdd.filter(lambda x: datetime.strptime(x[1],"%Y-%m-%d")<targetDay).cache()
    return rdd
    
def dist_days(target_day,day):
    dist=(datetime.strptime(target_day,"%Y-%m-%d") - datetime.strptime(day,"%Y-%m-%d")).days
    return dist

def dist_hours(target_hour,time):
    dist=abs( (datetime.strptime(target_hour,"%H:%M:%S")-datetime.strptime(time,"%H:%M:%S")).seconds )/3600.
    
    return dist


def kernel_func(diff_km, diff_days, diff_hours):
    kernel_diffKm = exp(-(diff_km/h_distance)**2)
    kernel_diffDays = exp(-(diff_days/h_date)**2)
    kernel_diffHours = exp(-(diff_hours/h_time)**2)
    kernel_sum= kernel_diffKm+kernel_diffDays+kernel_diffHours
    kernel_mult= kernel_diffKm*kernel_diffDays*kernel_diffHours
    return kernel_sum

#imput filter_dateRdd
rdd_stations=stations.map(lambda line :line.split(";"))
rdd_temps=temps.map(lambda line :line.split(";"))

# filter the temperature rdd 
rdd_temps_filtered=filter_dateRdd(date,rdd_temps)
# make rdd with station number and distances in km
rdd_stations_dist = rdd_stations.map(lambda x: ( x[0],( haversine(float(a),float(b),float(x[3]),float(x[4]))) ))


# make rdd with distance in days
rdd_temps_filtered_dist_days = rdd_temps_filtered.map(lambda x: ( x[0] ,(x[1],x[2],x[3],dist_days(date,x[1])) ))


joined_rdd=rdd_temps_filtered_dist_days.join(rdd_stations_dist)
# compute all distinces km, days and hours

for time in times:
    #step1
    kernel_rdd = joined_rdd.map(lambda x: ( x[1][0][2], kernel_func(x[1][1],x[1][0][3],dist_hours(time,x[1][0][1]))) )
    k=kernel_rdd.map(lambda x : (float(x[0])*x[1],x[1]) ) 
    reduced=k.reduce(lambda x,y: (x[0]+y[0],x[1]+y[1]) )
    prediction=reduced[0]/reduced[1]
    print(time,prediction)

