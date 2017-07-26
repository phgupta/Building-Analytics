"""
drafty draft for LBNL

## this class imports the data from one or multiple .csv
## Initially this will work for building-level meters data
## Initially this will work with .csv files, then it will incorporate the Lucid API (or others)
## --- Functionality with .csv
## Input (args): Specify file name and path, specify (via config file?) name mapping to type of meters
## ?? does this work for a single file or a whole directory ??
## right now we want date_time in the first column !!!
## Output (return): data in a dataframe, metadata table
## Note: may want to have a separate class for data + metadata




V0.1 
- works fine, not tested extensively

V0.2
- added: cast numeric on columns that are "object"

TO DO:

- add concatenate files 

- remove the main details 

@author Marco Pritoni <marco.pritoni@gmail.com>
@author Jacob Rodriguez  <jbrodriguez@ucdavis.edu>

last modified: July 24 2017
@author Correy Koshnick <ckoshnick@ucdavis.edu>
"""

import os
import pandas as pd
import numpy as np
import timeit

class csv_importer(object):

####################################################################################################################################    
    def __init__(self,
                 fileName=None,
                 folder=None,
                 folderAxis = 'concat',
                 fileAxis = 'merge',
                 headRow=0,
                 indexCol=0,
                 convertCol=True # convertCol specifies if user wants data to all be of numeric type or not. Default is convert to numeric type
                ):
        
        # the data imported is saved in a dataframe
        self.data=pd.DataFrame()
        self.tempData = pd.DataFrame()
        
        
        self.folderAxis = folderAxis.lower()
        self.fileAxis = fileAxis.lower()
        
        # loads the TS file or multiple files - it also converts timestamps in date_time format
        # if no info is provided it assumes the first column is the index and the first row is the header
        
        ### CHanges to make ###
        
        
        # done: look through folders first, so check is instance folder -> list first
        # done: if only 1 folder, handle files with fileAxis='m' or fileAxis='c'
        
        # done: if many folders, but only 1 file handle with folderAxis = 'm' or folderAxis = 'c'
        
        
        # do If many files in many folders, handle with m x n routine (yet to plan or implement)
            # m x n routine plan
            # If folderAxis = fileAxis --> Simple. open all, merge or concat
            
            #if folderAxis = C and fileAxis = M (MOST COMMON CASE!!)
                # Check files input to generate unique list
                # try open file in each folder, concat them all in tempDF
                # After finishing tempDF merge to mainDF
                # repeat for all file names
                
            #if FolerAxis = M and FileAxis = C (IGNORE THIS CASE FOR NOW, SHOULD BE RARE!!)
        
        if isinstance(folder, list): #########  MANY FOLDER CASES ############
            if isinstance(fileName, list): #MANY FOLDER MANY FILE 
                _fileList = []
                # If many files in many folders, handle with m x n routine (yet to plan or implement)
                for i, folder_ in enumerate(folder):
                    for j, file_ in enumerate(fileName):
                        
                        _fileList.append(file_)
                
                        # Check files input to generate unique list
                        # Use this list to determine the shape of mainDF
                _fileList = list(set(_fileList))
                
                for i, folder_ in enumerate(folder): ##REWORK THESE TO MATCH ABOVE ANALYSIS OF WHICH FILES LIVE OR DIE
                    for j, file_ in enumerate(fileName):##REWORK THESE TO MATCH ABOVE ANALYSIS OF WHICH FILES LIVE OR DIE
                            # try open file in each folder, _combine them all to tempDF
                            # repeat for all file names
                            # After finishing tempDF merge to mainDF                           
                            # clean tempDF and move to next folder
                                    
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
                    self.tempData = pd.DataFrame()
                
                pass
    
            else:  #MANY FOLDER 1 FILE CASE
                print "#MANY FOLDER 1 FILE CASE"
                
                # if many folders, but only 1 file handle wifth olderAxis = 'm' or folderAxis = 'c'
                
                for i, folder_ in enumerate(folder):
                    _headRow,_indexCol = self._head_and_index(headRow,indexCol,i)
#                    try:
                    newData = self._load_csv(fileName,folder_,_headRow,_indexCol,convertCol)
                    self.tempData = self._combine(self.tempData,newData, direction = self.folderAxis)
#                    except:
#                        print "Something went wrong with loading file %s " %file_
                self.data = self.tempData  
               
        else: ###################### SINGLE FOLDER CASES  #####################
            
            if isinstance(fileName, list): #### 1 FOLDER MANY FILES CASE  ####
            
                print "#1 FOLDER MANY FILES CASE"
                for i, file_ in enumerate(fileName):
                    _headRow,_indexCol = self._head_and_index(headRow,indexCol,i)        
#                    try:
                    newData = self._load_csv(file_,folder,_headRow,_indexCol,convertCol)
                    self.tempData = self._combine(self.tempData,newData, direction = self.fileAxis)
#                    except:
#                        print "Something went wrong with loading file %s " %file_
                self.data = self.tempData
            
            else: #### SINGLE FOLDER SINGLE FILE CASE ####
                print "#1 FOLDER 1 FILE CASE"
                self.data=self._load_csv(fileName,folder,headRow,indexCol)

###############################################################################
    def _combine(self,
                 oldData,
                 newData,
                 direction
                 ):
        '''
        This function uses merge or concat on newly loaded data 'newData' with the self.tempData storage variable
        '''
        if oldData.empty == True:
            return newData
        else:   
            
            if direction == 'merge':
                return pd.merge(oldData,newData,how='outer',left_index=True,right_index=True,copy=True)
            elif direction == 'concat' or direction.lower == 'concatentate':
                return pd.concat([oldData,newData],copy=True)
                
        
    def _head_and_index(self,headRow,indexCol,i):
        # to accept different head and index for each file - following the order in the fileName array
        # example call CSV_Importer( [file1,file2], folder, headRow=[0,4], indexCol=[0,1])
        if isinstance(headRow, list):
            _headRow=headRow[i]
        else:
            _headRow=headRow 

        if isinstance(indexCol, list): 
            _indexCol=indexCol[i]
        else:
            _indexCol=indexCol 
            
        return _headRow,_indexCol
                    

        
####################################################################################################################################
    def _load_csv(self, 
                  fileName,
                  folder,
                  headRow,
                  indexCol,
                  convertCol
                 ):
        
        #start_time = timeit.default_timer()
        #print fileName
        
        if fileName:
            try:
                folder = os.path.join('..',folder) # Appending onto current folder to get relative directory
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
        else: 
            print 'NO FILE NAME'
            return
    
            if convertCol == True: # Convert all columns to numeric type if option set to true. Default option is true.
                for col in data.columns: # Check columns in dataframe to see if they are numeric
                    if(data[col].dtype != np.number): # If particular column is not numeric, then convert to numeric type
                          data[col]=pd.to_numeric(data[col], errors="coerce")

        
        return data
####################################################################################################################################
if __name__=='__main__':
    
    start_time = timeit.default_timer()
    # code you want to evaluate
    
    folder=['test2','test3']

    fileName=["data1.csv","data3.csv"]
    rows = [0,4]
    cols = 0

    p = csv_importer(fileName,folder,headRow=rows,indexCol=cols,folderAxis='merge',fileAxis = 'concat')
    
    print(p.data.shape)

#
#    # rename for modeling - OAT name is hard coded in the code
#    print p.data.head(10)
#    
#    p.data.rename(columns = {'Temp_Avg':'OAT'}, inplace=True)
#
#    elapsed = timeit.default_timer() - start_time
#    print elapsed