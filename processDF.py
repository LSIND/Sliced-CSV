#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import os, time, random
from writefilesfolders import create_folders, create_files


def random_Hour(df, dtcol):
    '''Adds random HH (from 7:00 to 19:00) to datetime Series of Dataframe
    Args:
        df (pandas dataframe): dataframe with index col of datetime
    Returns:
        dataframe column with random hours of datetimeindex
    '''
    time_random = {k: k.replace(hour=random.randint(7, 19)) for k in df[dtcol]}
    df[dtcol] = df[dtcol].replace(microsecond=0) - pd.Timedelta('30H')
    df[dtcol] = df[dtcol].map(time_random)
    return df[dtcol]



def read_original(filepath):
    '''Creates groups in memory based on original concept: every group is a pandas dataframe
       Calls process_slices to write data on disk
       Args:
            filepath (str): fullpath of original concept
       Returns: 
            TIME (s) working with 1 concept file; NUMBER of new files; NUMBER of appended files
    '''
    base = os.path.basename(filepath)
    # print(os.path.basename(filepath))
    start = time.time()
    with open(filepath, 'r', encoding='cp1251') as f:
        header = f.readline()                         # header of original concept (1st line), f.e. T02_organization;Организация
    
    # READ AND CLEAN
    conc_df = pd.read_csv(filepath, sep = ';', engine='c', encoding='cp1251', comment='\x1A', dtype=object, skiprows = 1)
    conc_df = conc_df[conc_df.iloc[:,-1].notna()]   # drop NA rows
    print('{0}\n\tSize: {1} MB\t\t\t{2} rows'.format(base, round(os.stat(filepath).st_size/1024/1024, 2), conc_df.shape[0]))
    #print(conc_df)
    
    dtcolumn = conc_df.columns[-1]                        # get name of last column with datetime (object type)
    for col in conc_df.columns[0:2]:                      # convert id, idc to integer
        conc_df[col] = conc_df[col].astype(int)
    
    date_cache = {k: pd.to_datetime(k) for k in conc_df[dtcolumn].unique()}
    conc_df[dtcolumn] = conc_df[dtcolumn].map(date_cache)
    conc_df[dtcolumn] = conc_df[dtcolumn].dt.tz_localize('UTC').dt.tz_convert('Europe/Berlin')
    
    # unocomment this if u need to create random time of datetime object (ts col)
    # conc_df[dtcolumn] = random_Hour(conc_df, dtcolumn)
                                                 
    conc_df = conc_df.set_index(dtcolumn)       # index on datetime  
    dfgrouped = conc_df.resample('5BH')         # GROUP BY TS with resample - 5 business hours
       
    # u can try Grouper with resample; btw resample on DataFrame works a little faster
    #dfgrouped = conc_df.groupby(pd.Grouper(level=dtcolumn, freq='5BH'), squeeze = True, sort = False) # group by datetime 5business hours. Last column is a dt type!
    #dfgrouped = conc_df.groupby(dtcolumn).resample('5BH')

    rgtime = round(time.time() - start, 2)
    print('{0}\n\tT of read + group YMD HM:\t{1} s'.format(base, rgtime),end='\n')
    
    # CALL process_slices(): filepath and Resampler obj

    numofgroups, numofapp = process_slices(header, dfgrouped) #preparing to slice and create... filepath
    
    # RETURNS: TIME (s) working with 1 concept file; NUMBER of new files; NUMBER of appended files
    return (rgtime, numofgroups - numofapp, numofapp)
    
    
def process_slices(header, dfgrouped, ftype = 'csv'):
    '''Find number of groups for each concept, full name of file and call create_folders and create_files
    Args:
        header (str): 1st line from Original concept, f.e. "T02_organization;Организация"
        dfgrouped (Resampler obj / group by)
        ftype (str): '.csv' by default
    Returns:
        time of writing, number of groups, number of appended files
    '''
    numofgroups = dfgrouped.ngroups
    numappfiles = 0
    #concept = os.path.split(filepath)[-1].split('.')[0]
    concept = header.split(';')[0]
    concrus = header.split(';')[1]

    try:
        for index, group in dfgrouped:
            if group.empty:
                numofgroups -=1
                #pass
            else:
                #group = group.drop(group.columns[-1], axis=1)
                partfname, curpath = create_folders(index)               # ddmmYYYY_HHMM for filename, f.e. "18032020_1435"
                                                                         # path to write file, f.e. "../data/2020/032020/18032020"
                
                fslice = '{0}_{1}.{2}'.format(concept, partfname, ftype) # CONCEPT_ddmmYYYY_HHMM.csv
                filepath = os.path.join(curpath, fslice)
                numappfiles += create_files(filepath, concrus, group)    # create new or append to existing file for every group
                                                                         # get number of appended
        print('{0}\n\tNum of created  files:\t{1} files'.format(concept, numofgroups - numappfiles))
        print('\tNum of appended files:\t{0} files'.format(numappfiles))
    except Exception as e:
        print(e)
    return numofgroups, numappfiles