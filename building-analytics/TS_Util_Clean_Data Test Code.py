# -*- coding: utf-8 -*-
"""
Created on Wed Jul 26 10:13:10 2017

@author: Armando Casillas
"""

# inputs
fileName = "dandata2.csv"
folder = "data"

## call script

# instantiate class
TSU = TS_Util()

# load data
data= TSU.load_TS(fileName, folder)

# time zone + daylight stavings
#print TSU._utc_to_local(data)
print TSU._local_to_utc("2017-07-11")

# clean start-end
data= TSU.remove_start_NaN(data)
data= TSU.remove_end_NaN(data)

# find missing data
#selector = TSU.find_missing(data).any(axis=1)
#selector

TSU.display_missing(data, "any")

print TSU.count_missing(data,"number")

#TSU.display_outOfBound(data,lowBound=0,highBound=9998)

print data.shape



data.plot(figsize=(18,5))

#High bound set at 10000 (tonhr data should not exceed this for any given building, but should be an adjustable value)

data1 = TSU.remove_outOfBound(data,lowBound=0,highBound=10000)
    
print data1.shape


data1.plot(figsize=(18,5))

#Removes missing data when all columns are not present|

data2= TSU.remove_missing(data1,how="all")

print data2.shape

#data2.plot(figsize=(18,5))

print data2.max(axis=0)
print data2.idxmax(axis=0)


#Testing 4 main methods here

std = TSU.remove_outliers(data2,method="std", coeff= 2.5)

rstd = TSU.remove_outliers(data2,method="rstd", coeff= 2.5)

rmedian = TSU.remove_outliers(data2,method="rmedian", coeff= 500)

iqr = TSU.remove_outliers(data2,method="iqr", coeff= 3.0)


std.plot(figsize=(18,5))

rstd.plot(figsize=(18,5))


rmedian.plot(figsize=(18,5))

iqr.plot(figsize=(18,5))
