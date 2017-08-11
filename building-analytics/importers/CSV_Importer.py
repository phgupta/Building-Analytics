"""
## this class imports the data from one or multiple .csv files
## Initially this will work for building-level meters data
## Initially this will work with .csv files, then it will incorporate the Lucid API (or others)
## --- Functionality with .csv

## Output (return): data in a dataframe, metadata table [[[does not return meta data at the moment]]]
## Note: may want to have a separate class for data + metadata

V0.1 
- works fine, not tested extensively

V0.2
- added: cast numeric on columns that are "object"

@author Marco Pritoni <marco.pritoni@gmail.com>
@author Jacob Rodriguez  <jbrodriguez@ucdavis.edu>

V0.3
- added functionality where multiple folders and files may be specified
- handles case where not all files are present in all folders, but the program still runs and fills missing data with NaN
- added folderAxis / fileAxis direction functionalities
- added functions: _combine, _head_and_index
- added _utc_to_local function from TS_Util_Clean_Data to convert the UTC time (added pytz import to function properly)
- added index fixing features:
    -__init__ will now sort the df.index after all data has been loaded in self.data 
    -__init__ will now combine duplicate df.index indicies as the MEAN of the duped values


TO DO:
    - meta data
    - what if I want to have different headers for different files (currently the header input header = [0,2,3] will skip rows 0,2,3 from all files that are being loaded)
    - add robust test cases
    - improve speed (?)

last modified: August 11 2017
@author Correy Koshnick <ckoshnick@ucdavis.edu>
"""

import os
import pandas as pd
import numpy as np
import timeit
import pytz

class csv_importer(object):

####################################################################################################################################    
    def __init__(self,
                 fileNames=None,
                 folders=None,
                 folderAxis = 'concat',
                 fileAxis = 'merge',
                 headRow=0,
                 indexCol=0,
                 convertCol=True
                ):
        '''
        When initializing this class it will do the following:
            -Scan the input folder/file structure to determine if there is a single/many files or a single/many folders
            -Manages headRow indexCol sizes with function _head_and_index
            -Loads data from CSV into temp DataFrame until it is properly shaped
            -Once shaped it combined temp DataFrame with main DataFrame
            -Stores final data in self.data
            
        # DOES NOT HANDLE THE list of list for headRow indexCol idea yet. Maybe we wont use that for this case?  
   
        Parameters
        ----------
        fileNames: List of strings or string
            specify file name(s) that will be loaded from the folder strucuture passed in

        folders: List of strings or string
            The path(s) that will be searched for the above file(s)
            
        folderAxis: string = 'merge' or 'concat'
            The direction that the dataframes will be combined based on the folder to folder relationship
            default = 'concat' assuming the folder-folder relationship is a timeseries
            
        fileAxis: string = 'merge' or 'concat'
            The direction that the dataframes will be combined based on the folder to folder relationship
            default = 'merge' assuming the file-file relationship is different data meters for the same timeframe
            
        headRow: List of int or int
            Choose which rows to skip as the header when loading CSV files. A list will pass the
            headRow index with the corresponding file using the _head_and_index function
            
        indexCol: int
            which column from the file is the index, all merged dataframes will be merged on the index (dateTime index)
            
        convertCol: bool 
            convertCol specifies if user wants data to all be of numeric type or not. Default is convert to numeric type           folders: Dataframe


        Returns
        -------
        data: Dataframe
            Pandas dataframe with timestamp index adjusted for local timezone

        '''
        # the data imported is saved in a dataframe
        self.data=pd.DataFrame()
        self.tempData = pd.DataFrame()        
        
        self.folderAxis = folderAxis.lower()
        self.fileAxis = fileAxis.lower()
        
        if isinstance(headRow,list):
            assert(len(headRow) == len(fileNames))
        else:
            print('headRow length must match fileNames length as the header '
                  'rows are applied 1-to-1 with the files listed in fileNames!')
    
        if isinstance(folders, list): #########  MANY FOLDER CASES ############
            if isinstance(fileNames, list): # MANY FOLDER MANY FILE 
               
                ###--##--## THIS CODE SHOULD BE REMOVED
                _fileList = []
                # Check files input to generate unique list
                for i, folder_ in enumerate(folders):
                    for j, file_ in enumerate(fileNames):
                        _fileList.append(file_)
                _fileList = list(set(_fileList))
                ###--##--## END CODE REMOVAL SECTION
                
                for i, folder_ in enumerate(folders):
                    for j, file_ in enumerate(fileNames):
                                    
                        # DOES NOT HANDLE THE list of list for headRow indexCol idea yet. Maybe we wont use that for this case?
                       
                        _headRow,_indexCol = self._head_and_index(headRow,indexCol,j)
                        
                        #If folderAxis = fileAxis. Simple _combine
                        if self.folderAxis == self.fileAxis:
                            newData = self._load_csv(file_,folder_,_headRow,_indexCol,convertCol)
                            self.tempData = self._combine(self.tempData,newData,self.fileAxis)
                        #if folderAxis = C and fileAxis = M (MOST COMMON CASE!!)
                        if self.folderAxis == 'concat' and self.fileAxis == 'merge':
                            newData = self._load_csv(file_,folder_,_headRow,_indexCol,convertCol)
                            self.tempData = self._combine(self.tempData,newData,self.fileAxis)
                        #if FolerAxis = M and FileAxis = C 
                        if self.folderAxis == 'merge' and self.fileAxis == 'concat':
                            newData = self._load_csv(file_,folder_,_headRow,_indexCol,convertCol)
                            self.tempData = self._combine(self.tempData,newData,self.fileAxis)
                    
                    self.data = self._combine(self.data,self.tempData,direction=self.folderAxis)
                    
                    self.tempData = pd.DataFrame() #Reset temp data to empty
                    
            else:   #### MANY FOLDER 1 FILE CASE ####
                for i, folder_ in enumerate(folders):
                    _headRow,_indexCol = self._head_and_index(headRow,indexCol,i)
                    newData = self._load_csv(fileNames,folder_,_headRow,_indexCol,convertCol)
                    self.tempData = self._combine(self.tempData,newData, direction = self.folderAxis)
                self.data = self.tempData  
               
        else: ###################### SINGLE FOLDER CASES  #####################
            
            if isinstance(fileNames, list): #### 1 FOLDER MANY FILES CASE  #####
                for i, file_ in enumerate(fileNames):
                    _headRow,_indexCol = self._head_and_index(headRow,indexCol,i)        
                    newData = self._load_csv(file_,folder,_headRow,_indexCol,convertCol)
                    self.tempData = self._combine(self.tempData,newData, direction = self.fileAxis)
                self.data = self.tempData
            
            else: #### SINGLE FOLDER SINGLE FILE CASE ####
                print "#1 FOLDER 1 FILE CASE"
                self.data=self._load_csv(fileNames,folders,headRow,indexCol)
        
        
        #Last thing to do: remove duplicates and sort index
        self.data.sort_index(ascending=True,inplace=True)
        
        #For speed should it group by then sort or sort then groupby?
        #sorting is faster on a smaller object, but sorting may help groupby
        #scan the df faster, and groupby is more complicated, so it probably scales poorly
        
        #Removes duplicate index values in 'Timestamp' 
        #TODO should make the 'Timestamp' axis general and not hardcoded
        self.data = self.data.groupby('Timestamp',as_index=True).mean()
        
        # Convert timezone
        # TODO; should ensure a check that the TZ is convert or not converted??
        self.data = self._utc_to_local(self.data)
        

#### End __init__
###############################################################################

    def _utc_to_local(self,
                      data,
                      local_zone="America/Los_Angeles"):
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

  
    def _combine(self,
                 oldData,
                 newData,
                 direction
                 ):
        '''
        This function uses merge or concat on newly loaded data 'newData' with the self.tempData storage variable
        
        Parameters
        ----------
        oldData: Dataframe
            pandas dataframe usually 'self.tempData

        newData: Dataframe
            pandas datafrom usually newly loaded data from _load_csv()
            
        direction: string
            The axis direction stored in self.folderAxis or self.fileAxis which
            dictates if the two dataframes (oldData and newData) will be combined
            with the pd.merge or pd.concat function. 
            
            'merge' will perform an outer merge on left_index = True and
            right_index = True
            
            'concat' will preform a simple pd.concat

        Returns
        -------
        data: Dataframe
            Joined pandas dataframe on the two input dataframes. Usually then
            stored internally as self.tempData      
        '''
        if oldData.empty == True:
            return newData
        else:   
            
            if direction == 'merge':
                return pd.merge(oldData,newData,how='outer',left_index=True,right_index=True,copy=False)
            elif direction == 'concat' or direction.lower == 'concatentate':
                return pd.concat([oldData,newData],copy=False)              
        
    def _head_and_index(self,
                        headRow,
                        indexCol,
                        i):
        '''
        This function helps to manage the headRow variable as the files are being read.
        When the first file from fileNames is being opened by _load_csv this function will look
        at the corresponding self.headRows variable and self.indexCol variable and pass them into
        the _load_csv function
        
        Parameters
        ----------
        headRow: List of int or int
            Choose which rows to skip as the header when loading CSV files. A list will pass the
            headRow index with the corresponding file using the _head_and_index function
            
        indexCol: int
            which column from the file is the index, all merged dataframes will be merged on the index (dateTime index)
            
        i: int
            The index passed in from __init__ as it is iterating over the files in the fileNames

        Returns
        -------
        _headRow,_indexCol: int,int
            The corresponding values explained above
        '''
        if isinstance(headRow, list):
            _headRow=headRow[i]
        else:
            _headRow=headRow 
        if isinstance(indexCol, list): 
            _indexCol=indexCol[i]
        else:
            _indexCol=indexCol           
        return _headRow,_indexCol
                    
    def _load_csv(self, 
                  fileName,
                  folder,
                  headRow,
                  indexCol,
                  convertCol
                 ):
        '''
        Parameters
        ----------
        fileName: string
            specific file name that will be loaded from the folder

        folder: string
            The path that will be searched for the above file
            
        headRow: int
            Choose which rows to skip as the header when loading CSV files.
            
        indexCol: int
            which column from the file is the index, all merged dataframes will be merged on the index (dateTime index)
            
        convertCol: bool 
            convertCol specifies if user wants data to all be of numeric type or not. Default is convert to numeric type           folders: Dataframe

        Returns
        -------
        data: Dataframe
            newly loaded pd DataFrame from the CSV file passed in. usually immediately passed into _combine function
        '''        
        
        #start_time = timeit.default_timer()
        try:
            folder = os.path.join('..','..',folder) # Appending onto current folder to get relative directory
            path = os.path.join(folder,fileName)
            
            print "Current path is %s " %path
            
            if headRow >0:                
                data = pd.read_csv(path, index_col=indexCol,skiprows=[i for i in (range(headRow-1))]) # reads file and puts it into a dataframe                
                try: # convert time into datetime format
                    data.index = pd.to_datetime(data.index, format = '%m/%d/%y %H:%M') #special case format 1/4/14 21:30
                except:
                    data.index = pd.to_datetime(data.index, dayfirst=False, infer_datetime_format = True)             

            else:
                data = pd.read_csv(path, index_col=indexCol)# reads file and puts it into a dataframe
                try: # convert time into datetime format
                    data.index = pd.to_datetime(data.index, format = '%m/%d/%y %H:%M') #special case format 1/4/14 21:30
                except:
                    data.index = pd.to_datetime(data.index, dayfirst=False, infer_datetime_format = True)   

        except IOError:
              print 'Failed to load %s' %path + ' file missing!'
              return pd.DataFrame()      

    
        if convertCol == True: # Convert all columns to numeric type if option set to true. Default option is true.
            for col in data.columns: # Check columns in dataframe to see if they are numeric
                if(data[col].dtype != np.number): # If particular column is not numeric, then convert to numeric type
                      data[col]=pd.to_numeric(data[col], errors="coerce")
        return data
# END functions   
###############################################################################

def _test():
    start_time = timeit.default_timer()
    folder=['folder4','folder1']
    fileNames=["data1.csv"]
    rows = 0
    indexColumn = 0
    p = csv_importer(fileNames,folder,headRow=rows,indexCol=indexColumn,folderAxis='concat',fileAxis = 'merge')
    elapsed = timeit.default_timer() - start_time
    print p.data.head(10)
    print p.data.shape
    print elapsed, ' seconds to run'
    
    return p.data

if __name__=='__main__':
    A = _test()


