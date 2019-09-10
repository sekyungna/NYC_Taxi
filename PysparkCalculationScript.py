"""
Created on Thu Aug 15 07:17:00 2019

@author: nasekyung
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import datetime

import pandas
import numpy
import matplotlib.pyplot as plt


## Start Spark Session 
spark = SparkSession \
    .builder \
    .appName("Wrangling Data NY Taxi") \
    .getOrCreate()


## Set the year & month that you want to load & calculate 
taxi_type = "yellow"
year = "2018"    
MonthList = ["01", "02", "03", "04", "05", "06", "07", "08", "09","10", "11", "12"]


## Read the path that has the data saved (I used local path, but it can be database connections
for i in range(len(MonthList)):
    month = str(MonthList[i])
    path2 = "your_data_path_"+year+"-"+month+".csv"
    
    NYGreentaxi = spark.read.csv(path2, header = True)
    NYGreentaxi.createOrReplaceTempView("NYGreentaxi"+year+month+"_table")
    
    NYGreentaxi.printSchema()
    NYGreentaxi.take(1)


    ## Extract pickup data and do some calculation 
    pu_sql = spark.sql(
            """
            SELECT SUBSTRING(lpep_pickup_datetime, 1,13) AS Pickup_Time, 
            PULocationID AS Pickup_Location,
            SUM(total_amount) AS Total_Amount,
            AVG(total_amount) AS AVG_Total_Amount,
            SUM(trip_distance) AS Total_Trip_Distance,
            AVG(trip_distance) AS AVG_Trip_Distance,
            SUM(passenger_count) AS Total_Passenger_Count,
            AVG(passenger_count) AS Total_Passenger_Count,
            SUM(fare_amount) AS Fare_Amount,
            SUM(Extra) AS Extra,
            SUM(tip_amount) AS tip_amount,
            SUM(tolls_amount) AS tolls_amount,
            COUNT(VendorID) AS number
            FROM NYGreentaxi{}{}_table
            WHERE lpep_pickup_datetime IS NOT NULL
            and SUBSTRING(lpep_pickup_datetime, 1,7) = '{}-{}'
            GROUP BY SUBSTRING(lpep_pickup_datetime, 1,13), PULocationID
            ORDER BY Pickup_Time, Pickup_Location
            """.format(year, month, year, month)
            )
    
    ## Save the data calculated in a CSV format
    pu_sql.toPandas().to_csv("your_data_path" + year + month + taxi_type + "_NY_pickup.csv")
