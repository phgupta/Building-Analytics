# -*- coding: utf-8 -*-
"""
@author : Armando Casillas <armcasillas@ucdavis.edu>
@author : Marco Pritoni <marco.pritoni@gmail.com>

Created on Wed Jul 26 2017
Update Aug 08 2017

"""
import pandas as pd
import os
import sys
import requests as req
import json
import numpy as np
import datetime
import pytz
from pandas import rolling_median
from matplotlib import style
import matplotlib


class TS_Util(object):

    def load_TS(self, fileName, folder):
        '''      
        Parameters
        ----------

        Returns
        -------
        '''
        path = os.path.join(folder, fileName)
        data = pd.read_csv(path, index_col=0)
        data = self.set_TS_index(data)
        return data

    def set_TS_index(self, data):
        '''      
        Parameters
        ----------

        Returns
        -------
        '''
        # set index
        data.index = pd.to_datetime(data.index)

        # format types to numeric
        for col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")

        return data

    def _utc_to_local(self, data, local_zone="America/Los_Angeles"):
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
        data.index = data.index.tz_localize(pytz.utc).tz_convert(
            local_zone)  # accounts for localtime shift
        # Gets rid of extra offset information so can compare with csv data
        data.index = data.index.tz_localize(None)

        return data

    def _local_to_utc(self, timestamp, local_zone="America/Los_Angeles"):
        '''      
        Parameters
        ----------
        # Change timestamp request time to reflect request in terms of local time relative to utc - working as of 5/5/17 ( Should test more )
        # remove and add to TS_Util and import

        Returns
        -------
        '''

        timestamp_new = pd.to_datetime(
            timestamp, infer_datetime_format=True, errors='coerce')

        timestamp_new = timestamp_new.tz_localize(
            local_zone).tz_convert(pytz.utc)

        timestamp_new = timestamp_new.strftime('%Y-%m-%d %H:%M:%S')

        return timestamp_new

    def remove_start_NaN(self, data, var=None):
        '''      
        Parameters
        ----------

        Returns
        -------
        '''
        if var:  # limit to one or some variables

            start_ok_data = data[var].first_valid_index()

        else:

            start_ok_data = data.first_valid_index()

        data = data.loc[start_ok_data:, :]

        return data

    def remove_end_NaN(self, data, var=None):
        '''      
        Parameters
        ----------

        Returns
        -------
        '''
        if var:  # limit to one or some variables

            end_ok_data = data[var].last_valid_index()

        else:

            end_ok_data = data.last_valid_index()

        data = data.loc[:end_ok_data, :]

        return data

    def _find_missing(self, data):
        '''
        Function takes in pandas dataframe and find missing values in each column

        Parameters
        ----------
        data: Dataframe

        Returns
        -------
        data: Dataframe

        '''

        return data.isnull()

    def display_missing(self, data, how="any"):
        '''      
        Parameters
        ----------

        Returns
        -------
        '''

        if how == "any":

            bool_sel = self._find_missing(data).any(axis=1)

        elif how == "all":

            bool_sel = self._find_missing(data).all(axis=1)

        return data[bool_sel]

    def count_missing(self, data, how="number"):
        '''
        Parameters
        ----------
        how = "number" or "percent"

        Returns
        -------
        '''

        count = self._find_missing(data).sum()

        if how == "number":

            return count

        elif how == "percent":

            return count / (data.shape[0]) * 1.0 * 100

    def remove_missing(self, data, how):
        '''      
        Parameters
        ----------

        Returns
        -------
        '''

        if how == "any":

            bool_sel = self._find_missing(data).any(axis=1)

        elif how == "all":

            bool_sel = self._find_missing(data).all(axis=1)

        return data[~bool_sel]

    def _find_outOfBound(self, data, lowBound, highBound):
        '''      
        Parameters
        ----------

        Returns
        -------
        '''
        data = ((data < lowBound) | (data > highBound))

        return data

    def display_outOfBound(self, data, lowBound, highBound):
        '''      
        Parameters
        ----------

        Returns
        -------
        '''
        data = data[self._find_outOfBound(
            data, lowBound, highBound).any(axis=1)]

        return data

    def count_outOfBound(self, data, lowBound, highBound):
        '''      
        Parameters
        ----------

        Returns
        -------
        '''
        count = self._find_outOfBound(data, lowBound, highBound).sum()

        return count

    def remove_outOfBound(self, data, lowBound, highBound):
        '''      
        Parameters
        ----------

        Returns
        -------
        '''
        data = data[~self._find_outOfBound(
            data, lowBound, highBound).any(axis=1)]

        return data

    def _calc_outliers_bounds(self, data, method, coeff, window):
        '''      
        Parameters
        ----------

        Returns
        -------
        '''
        if method == "std":

            lowBound = coeff * data.mean(axis=0) - coeff * data.std(axis=0)
            highBound = coeff * data.mean(axis=0) + coeff * data.std(axis=0)

        elif method == "rstd":

       	    rl_mean=data.rolling(window=window).mean(how=any)
	    rl_std = data.rolling(window=window).std(how=any).fillna(method='bfill').fillna(method='ffill')

            lowBound = rl_mean - coeff * rl_std

            highBound = rl_mean + coeff * rl_std

        elif method == "rmedian":

            rl_med = data.rolling(window=window, center=True).median().fillna(
                method='bfill').fillna(method='ffill')

            lowBound =  rl_med - coeff
            highBound = rl_med + coeff

        elif method == "iqr":         # coeff is multip for std and IQR or threshold for rolling median

            Q1 = data.quantile(.25)     # coeff is multip for std or % of quartile
            Q3 = data.quantile(.75)
            IQR = Q3 - Q1

            lowBound = Q1 - coeff * IQR
            highBound = Q3 + coeff * IQR

        elif method == "qtl":

            lowBound = data.quantile(.005)
            highBound = data.quantile(.995)

        else:
            print ("method chosen does not exist")
            lowBound = None
            highBound = None


        return lowBound, highBound

    def display_outliers(self, data, method, coeff, window=10):
        '''      
        Parameters
        ----------

        Returns
        -------
        '''
        lowBound, highBound = self._calc_outliers_bounds(
            data, method, coeff, window)

        data = self.display_outOfBound(data, lowBound, highBound)

        return data

    def count_outliers(self, data, method, coeff, window=10):
        '''      
        Parameters
        ----------

        Returns
        -------
        '''
        lowBound, highBound = self._calc_outliers_bounds(
            data, method, coeff, window)

        data = self.count_outOfBound(data, lowBound, highBound)

        return count

    def remove_outliers(self, data, method, coeff, window=10):
        '''      
        Parameters
        ----------

        Returns
        -------
        '''
        lowBound, highBound = self._calc_outliers_bounds(
            data, method, coeff, window)

        data = self.remove_outOfBound(data, lowBound, highBound)

        return data

    def _find_equal_values(self, data, val):
        '''      
        Parameters
        ----------

        Returns
        -------
        '''                    
        bool_sel = data == val
            
        return bool_sel

    
    def _find_above_values(self, data, val):
        '''      
        Parameters
        ----------

        Returns
        -------
        '''                    
        bool_sel = data > val
            
        return bool_sel

    
    def _find_below_values(self, data, val):
        '''      
        Parameters
        ----------

        Returns
        -------
        '''                    
        bool_sel = data < val
            
        return bool_sel

    def count_if(self, data, condition, val, how="number"):
        
        """
        condition = "equal", "below", "above"
        val = value to compare against
        how = "number" or "percent"
        """
        if condition == "equal":
        
            count = self._find_equal_values(data,val).sum()

        elif condition == "above":

            count = self._find_above_values(data,val).sum()
      
        elif condition == "below":

            count = self._find_below_values(data,val).sum()

        if how == "number":
            
            return count
    
        elif how == "percent":
        
            return count/data.shape[0]*1.0*100

        return