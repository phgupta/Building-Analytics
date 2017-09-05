'''
This class is uses Lucid BuildingOS Rest API (reading) to extract data

This has been developed for the LBNL implementation and may not be general.
    

v0.1 
- this currently is just a bunch of functions, transform into class later
- this can:
    - create table of buildings with corresponding meters (can get only relevant meters)
        - save as csv
    - create table of meters with corresponding buildings
        - save as csv
    - given meter url get data (currently parsing is slow) - now works with one TS at the time

v0.2
- parsing TS now performs much better using native pandas tools to convert list of dict from the Json into a DataFrame
- implemented TS download 

v0.3
- transformed into class
- request token only when needed 
- can now get all 265 meters

v0.4
- added alpha version of writing API
- fixed TZ issue. Different download from API and download button

v0.5 
- make_table functions can also save and read from csv data (speed up), since metadata change very rarely

v0.6
- reorganized conde into sections: authorization, metadata, streams
- created  fuctions to search saved metadata and get stream data based on metadata
- modified get_meter_data to download multiple streams
- changed name of all methods that do not need to be accessed from outside as _method
- merged get_meter_byuuid and _get_meters into the same function
- merged get_building_byID and _get_buildings into the same function


TODO:
- GENERAL
   - clean and comment following https://www.python.org/dev/peps/pep-0008/    
- TS meters data:
    - figure out unit conversion (add units when reading from API)
- add default start and end time
- when retrieving TS change meter name with something more meaningful (BuildingOS_ID) - need to do a metadata query internally


BUGS:
- save table does not work properly
- make_meters_buildings behavior is weird chech how to get all the meters (maybe Reduced attribute plays a role)
- 

    
@author Marco Pritoni <marco.pritoni@gmail.com>
@author Jacob Rodriguez <jbrodriguez@ucdavis.edu>

latest update: May 30 2017 

'''
################################################################################################################################
# Update of 4/21/17
    # Pandas has option to increase number of rows displayed from table 'pd.options.display.max_rows' - can now see all meters
    # Confirmed that _parse_TS gives same information as parse_TS1. Removed parse_TS1 because is a slower implementation.
    # Added function post_to_DB. Getting a response of 200 but need to confirm is actually transmitting correct data to meter
    
    # TODO:
        # Confirm data is being correctly posted to meter in question
        # Generalize code to work with preexisting meters so can get and post to same meter
        
# Update for week of 4/26/17
    # Can now compare csv data with _parse_TS data because timeshift has been properly aligned. Solution took forever.
    
# Update for week of 5/5/17
    # Added function _adjust_DateTime to grab data needed in relation to our local time and not UTC
    # Added functionality to get_meter_dataDB - option SaveToCSV allows user to save data grabbed from server to local csv file
    
    # TODO:
        # Continue adding functionality to CSV save option, such as future readings coming from CSV and not server request.
        
# Update for week of 5/10/17
    # Added input folder where to save metadata data tables. Default folder="metadataDB" If specified folder doesn't exist, creates new one
    # Added readNew option to make_buildings_table. User can choose to read from saved table or query API for information.
            # Currently saving every query in folder/file combination. Check to see if only want to save sometimes.
    # Added readNew option to make_meters_table. Same logic as option for make_buildings_table.
    # Renamed make_meters_buildings to make_meters_buildings_table and adjusted each function call to include readNew = False.
        # Also assigned class varible self.meters_buildings_table to combined table information
    # Changed get_meter_dataDB to incorporate getting information from saved tables if requested by user with option readNew
        # Will attempt to load information from saved tables in class variables. If no information exist, will contact API for information
    
################################################################################################################################

from __future__ import division # Allows for true division operation on given items (no rounding)
import pandas as pd
import os
import requests as req
import json
import numpy as np
from pandas.tseries.offsets import *
import pytz
from datetime import date, datetime
import time
# CHANGED: Added import yaml
import yaml

import requests as req
import json


class lucidAPI_client(object):
   
    def __init__(self, access_token=None, 
                 refresh_token=None, 
                 url_root=None, 
                 fullTable=None, 
                 buildingMeters=None, 
                 saveTable=None,
                 folderName="metadataDB"):
        
        if url_root==None:
            self.url_root = "https://api.buildingos.com" # root that we are basing our searches upon
            
        if not os.path.exists(folderName): # If the currently selected folder doesn't exist, make a new folder
            self._folderName = os.makedirs(folderName)
        else:
            self._folderName = folderName
            
        self.access_token, self.refresh_token = self._get_token() # Getting tokens for rest of functions to use when authenticating
        self.buildings_table = pd.DataFrame() # Making class variable for later use to manage/hold table information
        self.meters_table = pd.DataFrame() # Making another class variable for later use/storage
        self.meters_buildings_table = pd.DataFrame() # Class variable to store combined table information

################################################################################################################################
#
#authorization methods
#                        
################################################################################################################################        
    

    def _get_token (self, fileName):

        """
        evenutally need read these variables from a config file
        
        Function posts to API with credentials to obtain access token and refresh token
        
        Parameters
        ----------
        None
        
        Returns
        -------
        access_token: string
            Unique authentication key used to query API
        
        refresh_token: string
            Unique authentication key used to post to API to obtain new access token once old one has expired
        """

        url = "https://api.buildingos.com/o/token/"
        
        # Default fileName = 'LucidAPI_Client_Credentials.yaml'
        with open(fileName, 'r') as obj:
            try:
                params = yaml.safe_load(obj)
                client_id = params['client_id']
                client_secret = params['client_secret']
                username = params['username']
                password = params['password']
                grant_type = params['grant_type']
                
            except yaml.YAMLError as exc:
                print(exc)
                                         
            
        '''
        params = {
        "client_id":"bbOY8n5IwKqVklv36rvOXU905uT626ENuVI9BFj1",
        "client_secret":"Gy4Clvpi2AUaPsRYyOIFlvrrSEPYOzvl93x6H5sTMteOICOvjrxnyajDI2EhrnIQNyboG5CBwBkJykgqkjlxR4FkPPvHUYBuVpMPHVpz9PiswJt9aTfEfqUVtoQ0Gdyq",
        "username":"mpritoni@lbl.gov",
        "password":"lbnlapi2",
        "grant_type":"password"
        }
        '''

        res = req.post(url, params)

        if res:
            access_token = res.json()["access_token"]
            refresh_token = res.json()["refresh_token"]
        else:
            print res

        return access_token, refresh_token

################################################################################################################################
    def _get_resources (self, 
                       url_root = "https://api.buildingos.com"):

        """
        This method is pretty useless : but is shows there are other things we may want to remove
        """

        headers = {"Authorization": "Bearer "+ self.access_token}

        res = req.get(url_root, headers=headers)
   
        if res.status_code == 403: # Need to get new token if true (Unauthorized)
            self.access_token, self.refresh_token = self._get_token() # retrieving new tokens
            headers = {"Authorization": "Bearer "+ self.access_token}
            res = req.get(url_root, headers=headers) #try request again with new access token

        return res

################################################################################################################################
#
#metadata methods
#
################################################################################################################################
    def _get_buildings (self,
                      ID=None,
                      url_start = "https://api.buildingos.com/buildings"
                      ):

        """
        This method gets a json with building metadata;
        
        Parameters
        ----------
        ID: string
            If an ID is specified it returns a single building otherwise all of them are returned
        
        url_start: string
            Root http path for query of API
            
        Returns
        -------
        res: http request body
            Requested information from http get function

        """

        if ID:
            url_building = url_start  + "/" + ID # ex: ID = "12404"
        else:
            url_building = url_start # get all buildings


        headers = {"Authorization": "Bearer "+ self.access_token}

        params = {"perPage":100} # needed otherwise it stops after 20 items

        res = req.get(url_building, headers=headers, params=params)
        
        if res.status_code == 403: # Need to get new token if true (Unauthorized)
            self.access_token, self.refresh_token = self._get_token()
            headers = {"Authorization": "Bearer "+ self.access_token}
            res = req.get(url_building, headers=headers, params=params) #try request again with new access token
        return res

################################################################################################################################
    def _parse_building_json(self):

        """
        Move here the parsing of the json
        """


        print "internal method is accessible"
        return

################################################################################################################################
    def _make_buildings_table (self,
                              readNew = False,
                              url_start = "https://api.buildingos.com/buildings",
                              fullTable = True,
                              buildingMeters = False,
                              saveTable = False
                             ):

        """
        This method creates a dataframe and save it as instance variable with some (not all) the metadata variables from buildings.
        The data can come from the LucidAPI parsing  the  json obtained by _get_buildings() or from the saved csv.
        It also save the same table as .csv if specified in the arguments
        
        Parameters
        ----------
        readNew: boolean
            Determines if data will be coming from saved local data(readNew=False) or from API get request (readNew=True)
        
        url_start: string
            Root http path for query of API
            
        fullTable: boolean
           Allows user to add more information to buildings_table: area, numFloors, and spaceType
           
        buildingMeters: boolean
            Grabs building meters url from response
        
        saveTable: boolean
            Allows user to save table information as csv with given filename of Buildings_Table.csv
            
        Returns
        -------
        building_df.T.sort_index(axis=1): pandas dataframe
            Pandas dataframe of given information transposed and sorted based on first axis
        
        """

        building_dic = {}
        building_df = pd.DataFrame()
        
        if readNew: # Want to query API for information instead of saved table
            response = self._get_buildings (url_start=url_start)


            # add example of the json parsed
            for idx, item  in enumerate(response.json()["data"]):

                building_dic["building_id"] = item["id"]
                building_dic["shortName"] = item["shortName"]
                building_dic["building_url"] = item["url"]
                if fullTable:
                    building_dic["area"] = item["area"]
                    building_dic["numFloors"] = item["numFloors"]
                    try:
                        building_dic["spaceType"] = item["buildingGroups"]["Space Type"]
                    except:
                        pass

                if buildingMeters:
                    for ix , itm  in enumerate(item["meters"]):
                        building_dic[ix] = item["meters"][ix]['url']

                curr_build_df = pd.DataFrame.from_dict(building_dic, orient='index')
                curr_build_df.rename(columns={0:item["name"]}, inplace=True)

                if building_df.empty:
                    building_df = curr_build_df
                else:
                    building_df = building_df.join(curr_build_df, how = "outer") 
             
            
            
            self.buildings_table = building_df.T # Saving table into class variable as transposed
            self.buildings_table.index.rename("building_index",inplace=True)

            
            if saveTable: # Saving quried table to folder/file when specified by saveTable variable
                fileName = "Buildings_Table.csv" # Need to find way to generate unique filename per save
                path=os.path.join(self._folderName,fileName) # saving to folder. Default _folderName is metadataDB
                building_df.T.sort_index(axis=1).to_csv(path) # Saving the CSV to current directory as transposed
                print 'Data has been saved as CSV with name: ' + fileName + ' in folder: ' + self._folderName
                #building_df.T.sort_index(axis=1).to_csv(fileName) # Saving the CSV to current directory
                
        else: # Want to grab table information from saved class table
            
            if self.meters_table.empty: # There is nothing to set meter_df to. Check two cases
                
                fileName = "Buildings_Table.csv"
                path=os.path.join(self._folderName,fileName)
                
                if os.path.isfile(path): # If we have the file saved as a csv, use that information
                    self.buildings_table = pd.read_csv(path, index_col=0) # read information in from saved csv as transposed
                    print "metadata coming from Buildings_Table.csv"
                    building_df = self.buildings_table.T # Assign to return variable as nontransposed
                    
                else: # There is nothing to set meter_df to. Need to query API
                    self.buildings_table = self._make_buildings_table ( fullTable = fullTable, 
                                                                  buildingMeters = buildingMeters, saveTable = saveTable, readNew = True) # gets information as transposed
                    print "No saved building information available. metadata coming from API"
                    building_df = self.buildings_table.T # Assign to variable as nontransposed
            
            else: # Table exist and will use info to assign to building_df
                print "metadata coming from saved building information"
                building_df = self.buildings_table.T # Assign to variable as nontransposed
                
        return building_df.T.sort_index(axis=1)

# ################################################################################################################################
    def _get_meters (self,
                      ID=None,
                      url_start = "https://api.buildingos.com/meters"
                      ):
            
        """
        
        This method gets a json with meter metadata;
        If an ID is specified it returns a single meter otherwise all of them

        """
        if ID:
            url_meter = url_start  + "/" + ID
        else:
            url_meter = url_start # + "?" + ID # check options

        headers = {"Authorization": "Bearer "+self.access_token}

        params = {"perPage":1000} # if not it stops after a few items

        res = req.get(url_meter, headers=headers, params=params)
        
        if res.status_code == 403: # Need to get new token if true (Unauthorized)
            self.access_token, self.refresh_token = self._get_token()
            headers = {"Authorization": "Bearer "+ self.access_token}
            res = req.get(url_building, headers=headers, params=params) #try request again with new access token

        return res

################################################################################################################################
    def _parse_meter_json(self):

        """
        Move here the parsing of the json
        """

        return

################################################################################################################################
    def _make_meters_table (self,
                           readNew = False,
                           url_start = "https://api.buildingos.com/meters",
                           saveTable = False
                             ):

        """
        This method creates a dataframe and save it as instance variable with some (not all) the metadata variables from meters.
        The data can come from the LucidAPI parsing  the  json obtained by _get_buildings() or from the saved csv.
        It also save the same table as .csv if specified in the arguments
        """

        meter_dic = {}
        curr_meter_df = pd.DataFrame()
        meter_df = pd.DataFrame()
        
        if readNew: # Query the API to get meter information
            response = self._get_meters(url_start=url_start)


            
            for idx, item in enumerate(response.json()["data"]):

                meter_dic["displayName"] = response.json()["data"][idx]["displayName"]
                meter_dic["BuildingOS_ID"] = response.json()["data"][idx]["name"]
                meter_dic["id_meter"] = response.json()["data"][idx]["id"]
                meter_dic["meter_url"] = response.json()["data"][idx]["url"]
                meter_dic["scope"] = response.json()["data"][idx]["scope"]["displayName"]
                meter_dic["resource"] = response.json()["data"][idx]["resourceType"]["displayName"]
                meter_dic["building"] = response.json()["data"][idx]["building"]
                meter_dic["integration"] = response.json()["data"][idx]["integration"]["displayName"]


                curr_meter_df = pd.DataFrame.from_dict(meter_dic, orient='index')
                curr_meter_df.rename(columns={0:item["name"]}, inplace=True)

                if meter_df.empty:
                    meter_df = curr_meter_df
                else:
                    meter_df = meter_df.join(curr_meter_df, how = "outer")  
            
            
            self.meters_table = meter_df.T # Saving table into class variable as transposed
            self.meters_table.index.rename("meter_index",inplace=True)
            
            if saveTable: # Saving quried table to folder/file when specified by saveTable variable
                fileName = "Meters_Table.csv" # Need to find way to generate unique filename per save
                path=os.path.join(self._folderName,fileName) # saving to folder. Default _folderName is metadataDB
                meter_df.T.to_csv(path) # Saving the CSV to current directory
                print 'Data has been saved as CSV with name: ' + fileName + ' in folder: ' + self._folderName
                #meter_df.T.to_csv(fileName)
                
        else: # Want to grab table information from saved class table
            
            if  self.meters_table.empty: # There is nothing to set meter_df to. Check two cases
                
                fileName = "Meters_Table.csv"
                path=os.path.join(self._folderName,fileName)
                
                if os.path.isfile(path): # If we have the file saved as a csv, use that information
                    self.meters_table = pd.read_csv(path, index_col=0) # read information in from saved csv as transposed
                    print "metadata coming from Meters_Table.csv"
                    meter_df = self.meters_table.T # Assign to return variable as nontransposed
                    
                else: # There is nothing to set meter_df to. Need to query API
                    self.meters_table = self._make_meters_table ( saveTable = saveTable, readNew = True) # Get value as transposed
                    print "No saved meter information available. metadata coming from API"
                    meter_df = self.meters_table.T # Assign to variable as nontransposed
                    
            else: # Table exist and will use info to assign to building_df
                print "metadata coming from saved meter information"
                meter_df = self.meters_table.T # Assign to variable as nontransposed
    

        return meter_df.T

################################################################################################################################
    def _make_meters_buildings_table (self, readNew=False,reduced=False, saveTable = False):


        """
        This method creates a dataframe and save it as instance variable with join of meters and buildings tables
        The data can come from the LucidAPI parsing  the  json obtained by _get_buildings() or from the saved csv.
        It also save the same table as .csv if specified in the arguments
        """

        # Both function calls now include option readNew. Each call has option set to False
        buildings_table = self._make_buildings_table ( 
                                             fullTable = True, 
                                             buildingMeters = False, 
                                             saveTable = False, 
                                             readNew=readNew) # Should these two calls always have readNew = False or be based on incoming?

        meters_table = self._make_meters_table ( 
                                          saveTable = False, readNew=readNew) 


        meters_buildings = meters_table.reset_index().merge(buildings_table.reset_index(), 
                                                             left_on="building", 
                                                             right_on="building_url" , how="left") # Check to see if this works
        

        if reduced:

            mask = (meters_buildings["scope"]=="Whole building") 
            mask = mask & ((meters_buildings["integration"]!="Energy Star") & (meters_buildings["integration"]!="Tridium"))
            meters_buildings = meters_buildings.loc[mask,:]
            meters_buildings.rename(columns={"index_x":"meter_name", "index_y": "building_name" }, inplace=True)


        if saveTable:

            meters_buildings.to_csv("meters_buildings.csv")
            
        self.meters_buildings_table = meters_buildings # Saving meter table information into class variable

        return meters_buildings

################################################################################################################################
    def _AND_condition_mask(self, metadata_table,dic_filter,key,mask):

        """
        simple method to construct the mask condition
        needs better abstraction
        """

        if mask.empty: # first condition of the series
            mask = (metadata_table[key]== dic_filter[key])
        
        else: # additional conditions need an & in front 
            mask = mask & (metadata_table[key]== dic_filter[key])
        
        return mask

################################################################################################################################
    def _OR_condition_mask(self, metadata_table,elem,key,inner_mask):
        
        """
        simple method to construct the mask condition
        needs better abstraction
        """

        if inner_mask.empty:                    
            inner_mask = (metadata_table[key] == elem)
            
        else: # additional conditions need an | in front 
            inner_mask = inner_mask | (metadata_table[key] == elem)
        
        return inner_mask

################################################################################################################################
    def _AND_masks(self, mask, inner_mask):
        
        """
        simple method to construct the mask condition
        needs better abstraction
        """

        if mask.empty:
            mask = inner_mask
            
        else:
            mask = mask & inner_mask
        
        return mask

################################################################################################################################
    def search_metadata(self, dic_filter=None, metadata_table=None, outputs="All"): 
        # get keys of dictionary passed
        
        """
        This method seraches the existing tables for matches of values provided by the users relative specific columns
        It mimics a metadata query
        it returns the fileterd table or a selection of columns of the filtered table
        """

        metadata_table = (metadata_table if metadata_table else self._make_meters_buildings_table(readNew=False, reduced=False, saveTable = False))


        if dic_filter: # if dict filter provided

            keys = dic_filter.keys()

            # construct mask unpacking the dictionary
            mask = pd.Series()

            # loop metadata keys (metadata types to filter on)
            for key in keys: 

                if isinstance(dic_filter[key], list): # if the values passed are in a list these are treated in OR condition

                    inner_mask =pd.Series()

                    for elem in dic_filter[key]: # for each element in the list 

                        inner_mask = self._OR_condition_mask(metadata_table,elem,key,inner_mask) # create a condition in or

                    mask = self._AND_masks(mask, inner_mask) # and add it to the mask

                else:

                    mask = self._AND_condition_mask(metadata_table,dic_filter,key,mask)
            
        else: # no filter is provided
            
            mask = pd.Series(np.ones(metadata_table.shape[0]).astype("bool")) # boolean index 
                       
        # output columns 
        if outputs == "All":
            
            return metadata_table.loc[mask,:]
            
        else:
            try:
                return pd.DataFrame(metadata_table.loc[mask,outputs])
            except:
                print "here"
                return metadata_table.loc[mask,:]

################################################################################################################################
    def get_meter_ID(self, dic_filter):    
        
        """
        This method uses metadata_serach and returns just the list of meter IDs that correspond to the query
        """

        return self.search_metadata(dic_filter,outputs="meter_url")["meter_url"].tolist()
        
################################################################################################################################
    def list_categories(self, metadata_table=None):
        
        """
        This method returns the column names of the metadata table
        """

        metadata_table = (metadata_table if metadata_table else self.meters_buildings_table)

        return metadata_table.columns.tolist()

################################################################################################################################
    def list_options(self, column, metadata_table=None):
        
        """
        This method returns unique values of a specified column in the metadata table
        """

        metadata_table = (metadata_table if metadata_table else self.meters_buildings_table)

        return metadata_table[column].unique().tolist()

################################################################################################################################
#
#time series methods
#
################################################################################################################################
    # this is much faster. Confirmed data was same as parse_TS1
    def _parse_TS (self, response, fullurls):

        """
        This method loads content of json response for a time series data of a single meter
        It also corrects for time zone
        """

        #pacific = pytz.timezone('US/Pacific') # Setting timezone for data grab
        df = pd.DataFrame(response.json()["data"])
        df.set_index(pd.to_datetime(df["localtime"]), inplace =True)
        df.drop("localtime", axis=1, inplace = True)
        
        # Temp
        df.drop("value", axis=1, inplace=True)

        
        #df = self._utc_to_local(df, pacific) # calling function to adjust timestamp index to reflect localtime
        #df.index = df.index.tz_localize(pytz.utc).tz_convert(pacific) # accounts for localtime shift.
        #df.index = df.index.tz_localize(None)# Gets rid of extra offset information so can compare with csv data
        df.columns=[fullurls]

        return df

################################################################################################################################
    def _utc_to_local(self, data, local_zone): # remove and add to TS_Util and import
        
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
    def _local_to_utc(self, timestamp, local_zone): # remove and add to TS_Util and import
        
        """
        This method loads content of json response for a time series data of a single meter
        It also corrects for time zone of the response
        """

        #pacific = pytz.timezone('US/Pacific') # Setting timezone for data grab
        
        timestamp_new = pd.to_datetime(timestamp, format='%Y-%m-%d', errors='coerce')
        #end_new = pd.to_datetime(end, format='%Y-%m-%d', errors='coerce') #Changing to datetime format so can convert to local time
        
        
        timestamp_new = timestamp_new.tz_localize(local_zone)#.tz_convert(pacific)
        #end_new = end_new.tz_localize('America/Los_Angeles')# pytz.utc .tz_convert(pacific) # Localizing times so request reflects PT time and not utc
        
        #start_new = start_new.tz_localize(None)
        #end_new = end_new.tz_localize(None)
        
        timestamp_new = timestamp_new.strftime('%Y-%m-%d %H:%M:%S')
        #end_new = end_new.strftime('%Y-%m-%d %H:%M:%S') # Converting datetime back to string for get request
        
        return timestamp_new#, end_new

################################################################################################################################
    def get_meter_data (self, # Code is currently work in progress 5/4/17
                        fullurls ,#='https://api.buildingos.com/meters/c3ee4b3ef3ba11e6a58c5254009e602c',
                        start , #="2016-12-01", 
                        end , #="2017-02-28", 
                        resolution="hour"):

        """
        This method gets time-series meter data from the API and returns value and response
        """

        df = pd.DataFrame()
        res = None

        #?start=&end=&limit=&order=&resolution=?start=YYYY-MM-DD&end=YYYY-MM-DD&order=&resolution=&limit=
        # see also other options
        #url_meter = "https://api.buildingos.com/meters/"+ID+"/data?"

        if isinstance(fullurls, list): # if a list of urls is sent

            for fullurl in fullurls:

                df_single, res_single = self.get_meter_data(fullurl, start, end, resolution)

                if not res_single: # if response is not <200>

                    pass

                elif df.empty and res_single:

                    df = df_single
                    res = res_single

                else:

                    df = df.join(df_single, how="outer")
                    res = res_single


        else:

            headers = {"Authorization": "Bearer "+self.access_token}
            
            # ****************************************************** Gutting here ******************************************************
            # Marco may want to give user option to not change dataTime. This is where code is executed.
            adjusted_start = self._local_to_utc(start, 'America/Los_Angeles')
            adjusted_end = self._local_to_utc(end, 'America/Los_Angeles') # adjusting time because requested timeframe is based on utc time
            
            params = {"start": adjusted_start, # Passing in adjusted times to reflect our location in relation to utc
                      "end": adjusted_end,
                      "resolution": resolution,
                      "include": "consumption" }
            
            url_meter = fullurls + "/data"
            res = req.get(url_meter, headers=headers, params=params)
            resCopy = res
            
            if res:
                df = self._parse_TS (res,fullurls)
                df = self._utc_to_local(df, 'America/Los_Angeles') # Adjusting timestamp
                last_date = df.tail(1).index # Getting last datetime index of dataframe
                
    # Checking if received all timeseries points that wanted to get. If didn't get all timeseries, use next link to get rest with loop
                while last_date < end:
            
                    try: # Not-so-elegant way of dealing with a next link of null when comparison is one off
                        res = req.get(res.json()["links"]["next"],headers=headers)
                        df1 = self._parse_TS(res, fullurls)
                        df1 = self._utc_to_local(df, 'America/Los_Angeles')
                        df = df.append(df1) # Appending other page of information onto current dataframe
                        last_date = df.tail(1).index # Grabbing new last index to check comparison
                    except:
                        break # exception raised when trying to follow null link for next page. Means there is no more pages to search
                        
            else:
                print "error in getting meter"

            return df, res

        return df, res

################################################################################################################################
#
#POST methods
#
################################################################################################################################
# Work-in-progress code for POST to DB (4/21/17) - working but need to confirm data update
    def post_to_DB(self, datasource, meterId, meterName, meterUnits, timeStamp, value):
        
        """
        This method attempts to post using the push API.
        Development interrupted at the moment
        
        """
        url = "https://rest.buildingos.com/json/readings/"# path for POST
        
        
        headers = {"Content-Type": "application/json"}
        body = json.dumps ({
            "datasource": datasource, #specified gateway that is given by buildingos
             #"constructCatalog": false,
             #"constructReadings": false # These two lines can be omitted when dealing with a preexisting meter (ie. don't need to construct a new Catalog)
                
            "meterCatalog": [
                            {
                            "meterId": meterId, # meter information that maps request to specified meter
                            "meterName": meterName,
                            "meterUnits": meterUnits
                            }
                            ],
             "readings": [
                            {
                            "timestamp": timeStamp, # which timestamp value to alter
                            "meterId": meterId, # specified meter to be updated
                            "value": value # what the new timestamp value will be
                            }
                         ]
                })

        res = req.post(url, data=body, headers=headers)
        print res # printing for now to see response from server until we know data is actually updated
        print res.json()

        