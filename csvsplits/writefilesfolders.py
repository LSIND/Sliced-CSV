#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# testing request

import pandas as pd
import os

def create_folders(date_obj):
    '''Create folders in a hierarchy (if not exist) based on date_obj
        root -- current work directory
            data/YYYY/mmYYYY/ddmmYYYY
    Args:
        date_obj (str): group, datetime as a string in format dd-mm-yyyy HH:MM:SS, f.e. "18-03-2020 14:35:00"
    Returns:
        fname: ddmmYYYY_HHMM for filename, f.e. "18032020_1435"
        curpath: path to write file, f.e. "../data/2020/032020/18032020"
    '''
    root = os.getcwd()    #root
    date_obj = date_obj.strftime('%d-%m-%Y %H:%M:%S')
    # Extracting data from date_obj dd-mm-yyyy HH:MM:SS, f.e. 18-03-2020 14:35:00
    date_obj = date_obj.replace('-','').replace(':','')     # ddmmyyyy HHMMSS, 18032020 143500
    y_fld = date_obj[4:8]                                   # yyyy,     2020
    my_fld = date_obj[2:8]                                  # mmyyyy,   032020
    dmy_fld = date_obj[:8]                                  # ddmmyyyy, 18032020
    hhmm_file = date_obj[-6:][:4]                           # hhmm,     1435
    #print(y_fld, my_fld, dmy_fld, hhmm_file)
    
    curpath = os.path.join(root, 'C:\\FULL\\dataJul', y_fld, my_fld, dmy_fld)
    
    # Create dirs if not exist based on curpath ..data/YYYY/mmYYYY/ddmmYYYY
    if not os.path.exists(curpath):                         # create folder if not exists
        os.makedirs(curpath)                                # data/YYYY/mmYYYY/ddmmYYYY
        #pass
    return dmy_fld + '_' + hhmm_file, curpath

def create_files(filepath, conceptrus, group):
    '''Create new files or append to existing one with name CONCEPT_ddmmYYYY_HHMM.csv
    Args:
        filepath (str): full filepath, f.e. "../data/2020/032020/18032020/CONCEPT_18032020_1435.csv"
        group (pandas Series): unique numbers for each data group
    Returns:
        numappfiles: number of appended files
    '''
    numappfiles = 0
    #concept = os.path.split(filename)[-1].split('.')[0] 

    if(os.path.exists(filepath)):
        with open(filepath, 'a', encoding='utf-8-sig', newline='') as f:
            #print(filename, end='\n')
            numappfiles += 1
            group.to_csv(f, index=False, header=False, encoding='utf-8-sig', sep = ';')
    else:
        with open(filepath, 'w', encoding='utf-8-sig', newline='') as f:
            f.write(conceptrus)
            group.to_csv(f, index=False, encoding='utf-8-sig', sep = ';')
    return numappfiles