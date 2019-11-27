from __future__ import absolute_import
import os, sys
if os.getcwd() in sys.path:
    sys.path.remove(os.getcwd())
import tables
import time
import indroid.input.csv as csv
import indroid.input.xml as xml
from indroid.input.csv import Source
import cStringIO, re, shutil, datetime, random, datetime
#import gevent, gevent.monkey
#gevent.monkey.patch_socket(aggressive=False)
#gevent.monkey.patch_all(thread=False, process=False)
import multiprocessing, subprocess, threading
import numpy as np
from indroid import pandas as pd
from tables import *
from collections import Counter
data_root = "X:\\"
data_root = "e:\\Gerard\\Dropbox\\"

class bizDataFrame(pd.DataFrame):
    def to_hdf(self, path_or_buf, key):
        super(bizDataFrame, self).to_hdf(path_or_buf, key)
        pass

class Cust(IsDescription):
    odin_id =Int32Col()
    name = StringCol(64)

def test_hdf_write_csv(file_in=r"{}data\source\kpn\swol_marketing\odin\2012-09-26\extract\odin_dump-packages-export-utf-8.csv",
                       file_store=r"{}data\source\kpn\swol_marketing\odin\2012-09-26\extract\pytables.h5"):
    global data_root
    file_store = file_store.format(data_root)
    file_in = file_in.format(data_root)
    f_in = Source(file_in).table(0)
    dfn = [(str(fieldname), 'S25') for fieldname in f_in.fieldnames]
    dfn = np.dtype(dfn)
    f = openFile(file_store, mode='w')
    root = f.root
    grp = f.createGroup(root, 'cust')
    tbl = f.createTable(grp, 'cust', dfn)
    tbl.append(f_in.rows())
    f.flush()
    f.close()
    

def test_hdf_write(file_store=r"{}data\source\kpn\swol_marketing\odin\2012-09-26\extract\pytables.h5"):
    global data_root
    file_store = file_store.format(data_root)
    # Set up some tables and fill them:
    f = openFile(file_store, mode='a')
    root = f.root
    try:
        grp = f.createGroup(root, 'cust')
        tbl = f.createTable(grp, 'cust', Cust)
    except:
        grp = f.getNode('/cust')
        tbl = getattr(grp, "cust")
    row = tbl.row
    for i in range(10000, 300000):
        row['odin_id'] = i
        row['name'] = 'Naam: {:0=5}'.format(i)
        row.append()
    print f
    f.flush()
    f.close()

def test_pandas(file_store=r"{}data\source\kpn\swol_marketing\odin\2012-09-26\extract\pytables.h5"):
    global data_root
    file_store = file_store.format(data_root)
    f = pd.read_hdf(file_store, "cust/cust")
    print f

if __name__ == '__main__':
    test_hdf_write_csv()
    #test_pandas()
    #test_hdf()