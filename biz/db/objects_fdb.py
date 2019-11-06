from __future__ import absolute_import
import os, sys
if os.getcwd() in sys.path:
    sys.path.remove(os.getcwd())
import fdb
import biz.input.csv as csv
import biz.input.xml as xml
from biz.input.csv_dialects import DialectCSV
import cStringIO, re, shutil, datetime, random, datetime
#import gevent, gevent.monkey
#gevent.monkey.patch_socket(aggressive=False)
#gevent.monkey.patch_all(thread=False, process=False)
import multiprocessing, subprocess
import threading

fdb.api_version(21)
from fdb.tuple import _encode, _decode

@fdb.transactional
def tableSetCell( tr, table, row, column, value ):
    tr[ fdb.tuple.pack( (table, row, column) ) ] = _encode(value)

@fdb.transactional
def tableGetCell( tr, table, row, column ):
    return _decode(tr[ fdb.tuple.pack( (table, row, column) ) ], 0)[0]

@fdb.transactional
def tableSetRow( tr, table, row, cols ):
    del tr[ fdb.tuple.range( (table, row, ) ) ]
    for c,v in cols.iteritems():
        tableSetCell( tr, table, row, c, v )

@fdb.transactional
def tableGetRow( tr, table, row ):
    cols = {}
    for k,v in tr[ fdb.tuple.range( (table, row, ) ) ]:
        t, r, c = fdb.tuple.unpack(k)
        cols[c] = _decode(v, 0)[0]
    return cols

def test_insert(db,
                file_in=r"P:\data\source\kpn\swol_marketing\odin\2013-05-29\extract\odin_dump.csv",
                max_rows=100):    
    for i, row in enumerate(csv.Source(file_in, max_rows=max_rows).table(0)):
        tableSetRow(db, 'test', i, row)
        if i % 333 == 0:
            print i, row

def test_read(db, max_rows):
    t0 = datetime.datetime.now()
    list = range(max_rows)
    reads = 0
    for i in list:
        row = tableGetRow(db, 'test', i)
        reads += len(row)
        if i % 1000 == 0:
            print i, row['odin_id'],
            secs = (datetime.datetime.now() - t0).total_seconds()
            print int(secs), int(reads/secs)

def test_read_gevent(db, max_rows):
    def read_rows(db, t, offset, increment, accu):
        for i in range(offset, max_rows, increment):
            gevent.sleep(0)
            accu.append(tableGetRow(db, t, i))
            print accu
    rows = []
    cnt = 2
    threads = [gevent.spawn(read_rows, db, 'test', offset, cnt, rows) for offset in range(cnt)]
    gevent.joinall(threads)
    return rows

class FdbRead(threading.Thread):
    def __init__(self, db, table, accu, offset, increment, max_rows):
        self.db = db
        self.table = table
        self.accu = accu
        self.offset = offset
        self.increment = increment
        self.max_rows = max_rows
        super(FdbRead, self).__init__()
    def run(self):
        for i in range(self.offset, self.max_rows, self.increment):
            self.accu.append(tableGetRow(self.db, self.table, i))
            if i % 1000 == 0:
                print i, datetime.datetime.now()

class FdbProcess(multiprocessing.Process):
    def __init__(self, thread_cnt, table, offset, increment, max_rows):
        self.db = fdb.open()
        self.thread_cnt = thread_cnt
        self.table = table
        self.offset = offset
        self.increment = increment
        self.max_rows = max_rows
        self.accu = []
        super(FdbProcess, self).__init__()
    def run(self):
        for i in range(self.offset, self.max_rows, self.increment):
            self.accu.append(tableGetRow(self.db, self.table, i))
            if i % 1000 == 0:
                print i, datetime.datetime.now()
    #def run1(self):
        #rows = []
        #threads = [FdbRead(db, self.table, rows, self.offset, self.thread_cnt, self.max_rows) for offset in range(self.thread_cnt)]
        #for thread in threads:
            #thread.start()
        #for thread in threads:
            #thread.join()
        #return rows

class FdbSubprocess(threading.Thread):
    def __init__(self, offset, increment, max_rows):
        self.offset = offset
        self.increment = increment
        self.max_rows = max_rows
        super(FdbSubprocess, self).__init__()
    def run(self):
        r = subprocess.check_output(["python", r"X:\code\biz\db\objects.py", r"P:\data\export\kpn\test{}.txt".format(self.offset)])
        print r

def test_read_threading(db, thread_cnt, max_rows):
    t0 = datetime.datetime.now()
    rows = []
    threads = [FdbRead(db, 'test', rows, offset, thread_cnt, max_rows) for offset in range(thread_cnt)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return rows

def test_read_subprocess(thread_cnt, max_rows):
    t0 = datetime.datetime.now()
    rows = []
    threads = [FdbSubprocess(offset, thread_cnt, max_rows) for offset in range(thread_cnt)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return rows

def test_read_process(process_cnt, thread_cnt, max_rows):
    # Todo: process the process-count.
    p = multiprocessing.Process(target=test_read, args=(fdb.open(event_model='gevent'), max_rows))
    p.start()
    p.join()

def init():
    return fdb.open()
    gevent.sleep(0.1)
    return fdb.open(event_model='gevent')

if __name__ == '__main__':
    max_rows = 100000
    db = init()
    #test_insert(db, max_rows=max_rows)
    t0 = datetime.datetime.now()
    rows = test_read_threading(db, 5, max_rows)
    #test_read_subprocess(100, max_rows)
    print datetime.datetime.now() - t0
