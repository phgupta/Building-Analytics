# -*- coding: utf-8 -*-
"""
Created on Wed Jul 26 09:33:24 2017

@author: Armando Casillas
"""


import pandas as pd
import os, sys
import requests as req
import json
import numpy as np
import datetime
import pytz
from pandas import rolling_median
from matplotlib import style
import matplotlib
%matplotlib inline
style.use('ggplot')


class TS_Util(object):
    
   # def __init__(self, data= None):
     #   self.data = data
        
        
    def load_TS(self, fileName, folder):

        # import file
        path = os.path.join(folder, fileName)
        data = pd.read_csv(path, index_col=0)
        
        # set index
        data.index = pd.to_datetime(data.index)
                
        #format types to numeric 
        for col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")

        return data
    
################################################################################################################################
    def _utc_to_local(self, data, local_zone="America/Los_Angeles"): # remove and add to TS_Util and import
        
        '''
        Function takes in pandas dataframe and adjusts index according to timezone in which is requested by user
        
        Parameters
        ----------
        data: Dataframe
            pandas dataframe of json timeseries response from server
        
        local_zone: string
            pytz.timezone string of specified local timezone to change index to
            
        Returns
        -------
        data: Dataframe
            Pandas dataframe with timestamp index adjusted for local timezone
        '''
        data.index = data.index.tz_localize(pytz.utc).tz_convert(local_zone) # accounts for localtime shift
        data.index = data.index.tz_localize(None)# Gets rid of extra offset information so can compare with csv data
        
        return data

################################################################################################################################
# Change timestamp request time to reflect request in terms of local time relative to utc - working as of 5/5/17 ( Should test more )
    def _local_to_utc(self, timestamp, local_zone="America/Los_Angeles"): # remove and add to TS_Util and import
        
        """
        This method loads content of json response for a time series data of a single meter
        It also corrects for time zone of the response
        """

        timestamp_new = pd.to_datetime(timestamp, infer_datetime_format=True, errors='coerce')
        
        timestamp_new = timestamp_new.tz_localize(local_zone).tz_convert(pytz.utc)        
        
        timestamp_new = timestamp_new.strftime('%Y-%m-%d %H:%M:%S')
        
        return timestamp_new
    
################################################################################################################################
    def remove_start_NaN(self, data, var=None):
        
        if var: # limit to one or some variables
            
            start_ok_data = data[var].first_valid_index()
            
        else:
            
            start_ok_data = data.first_valid_index()
            
        
        data = data.loc[start_ok_data:,:]
        
        return data


    def remove_end_NaN (self, data, var=None):

        if var: # limit to one or some variables
            
            end_ok_data = data[var].last_valid_index()
            
        else:
            
            end_ok_data = data.last_valid_index()
            
        
        data = data.loc[:end_ok_data,:]
        
        return data

################################################################################################################################    
    def find_missing(self, data):
        
        data_cond = data.isnull()
            
        return data_cond
    
    def display_missing(self, data, how="any"):
        
        if how == "any":
            
            bool_sel = data.isnull().any(axis=1)
            
        elif how == "all":
            
            bool_sel = data.isnull().all(axis=1)
            
        return data[bool_sel]
    

    def count_missing(self, data, how="number"):
        
        """
        how = "number" or "percent"
        """
        count = self.find_missing(data).sum()       
        
        if how == "number":
            
            return count
    
        elif how == "percent":
        
            return count/(data.shape[0])*1.0*100
        
    def remove_missing(self,data, how):
        
        if how == "any":
            
            bool_sel = data.isnull().any(axis=1)
            
        elif how == "all":
            
            bool_sel = data.isnull().all(axis=1)
            
        return data[~bool_sel]
    

#### Armando TO DO 

#### the two main funtions I worked on were remove_outliers and display_outliers. 
#### Both funtions employ the same methods but one cuts and the other will isolate and display outliers for analysis

    def find_outOfBound(self, data, lowBound, highBound):
    
        data=data.dropna()
    
    
        data= ((data < lowBound).any(axis=1) | (data > highBound).any(axis=1))
    

        return data


    
    def display_outOfBound(self, data, lowBound, highBound):

        data= self.find_outOfBound(data,lowBound,highBound)
    
        return data

    def count_outOfBound(self, data, lowBound, highBound):

        count = self.find_outOfBound(data, lowBound, highBound).sum()
    
        return count

    def remove_outOfBound(self,data,lowBound,highBound):
        
        data_oob = data[~((data < lowBound).any(axis=1) | (data > highBound).any(axis=1))]
        
        data = data_oob
        
        return data


    def remove_outliers(self,data, method, coeff): # coeff is multip for std and IQR or threshold for rolling median

        if method == "std":
        
            data = data[~(data > coeff* data.std(axis=0))]
            
            return data
        
        if method == "rstd":
    
            data = data[~(data > coeff*pd.rolling_std(data,window=168,how=any))]
            
            return data
        
        if method == "rmedian":
        
            
            rmdata= rolling_median(data, window= 168,center=True).fillna(method='bfill',axis=1).fillna(method='ffill',axis=1)

            difference = np.abs(data - rmdata)
            
            outlier_idx = difference > coeff

            data=data[~outlier_idx]
            
            return data
        
        if method == "iqr":
        
            Q1=  data.quantile(.25)
        
            Q3 = data.quantile(.75)
            
            IQR= Q3-Q1

            low = data.quantile(0.25)- coeff*IQR

            high=data.quantile(0.75) + coeff*IQR


            data = data[~(((data < low).any(axis=1)) |(( data > high).any(axis=1)))]
            return data
          
        
        elif method == "qtl":
    
            data = data[~((data < data.quantile(.005)).any(axis=1) | (data > data.quantile(.995)).any(axis=1))]

            return data
 
    
    
    def display_outliers(self,data, method="qtl", coeff=3.0):
   

        if method == "std":
        
            data = data[(data > coeff* data.std(axis=0))]
            
            return data
        
        if method == "rstd":
    
            data = data[(data > coeff*pd.rolling_std(data,window=168,how=any))]
            
            return data
        
        if method == "rmedian":
        
            
            rmdata= rolling_median(data, window= 168,center=True).fillna(method='bfill',axis=1).fillna(method='ffill',axis=1)

            difference = np.abs(data - rmdata)
            
            outlier_idx = difference > coeff

            data=data[outlier_idx]
            
            return data
        
        if method == "iqr":
        
            Q1=  data.quantile(.25)
        
            Q3 = data.quantile(.75)
            
            IQR= Q3-Q1

            low = data.quantile(0.25)- coeff*IQR

            high=data.quantile(0.75) + coeff*IQR


            data = data[(((data < low).any(axis=1)) |(( data > high).any(axis=1)))]
            return data
          
        
        elif method == "qtl":
    
            data = data[((data < data.quantile(.005)).any(axis=1) | (data > data.quantile(.995)).any(axis=1))]

            return data
 
 
    
    def count_outliers(self,data, method= "qtl", coeff=3.0): # coeff is multip for std or % of quartile

        count = self.display_outliers(data,method,coeff).sum()
    
        return count

