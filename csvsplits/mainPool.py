#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import multiprocessing as mp
import os, sys, time, random
from processDF import read_original

# py -3 mainPool.py "C:\Users\Projects\parsebytimestamp\out_2606"

def listdir_fullpath(d):
    return [os.path.join(d, f) for f in os.listdir(d)]


def parallel_processing(userpath):
    ''' Process 3 files in parallel'''
    files = listdir_fullpath(userpath)    
    random.shuffle(files)               # shuffle just in case; crutch

    with mp.Pool(3) as pool:            # 3 files go in parallel
        pool.map(read_original, files)


def seq_processing(userpath):
    files = os.listdir(userpath)
    ttime = nfiles = groups = 0
    for file in files:
        if file.endswith('.csv'):
            fulltime, numfiles, gr = read_original(os.path.join(userpath, file))
            ttime+=fulltime
            nfiles+=numfiles
            groups+=gr
        else:
            print('File {0} is not in csv format'.format(file))
    print('\nTotal time of execution:\t\t{0} s'.format(round(ttime,2)))
    print('Total new files created:\t\t{0}'.format(nfiles))
    print('Total ex files appended:\t\t{0}'.format(groups))

    
if __name__ == '__main__':
    userpath = ''
    
    start = time.time()
    print('Log started: {}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())))
    print('Pandas version: {}'.format(pd.__version__))
    print(50*'-', end='\n')
    if len(sys.argv)==2:
        userpath = sys.argv[1]
    else:
        print('input command parameter in format: py -3 main.py "C:\\Users\\Admin\\Downloads"')
    
    if os.path.isdir(userpath):
        parallel_processing(userpath)
        #seq_processing(userpath)
        print('Log ended: {}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())))
        print(50*'-', end='\n')
        print('\nTotal time of execution:\t\t{0} s'.format(round(time.time() - start, 2)))
    else:
        print('Folder {0} doesn\'t exist'.format(userpath))
