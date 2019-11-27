from __future__ import absolute_import
import os, sys
if os.getcwd() in sys.path:
    sys.path.remove(os.getcwd())
import indroid.input.csv as csv
import indroid.input.xml as xml
from indroid.input.csv_reader import UnicodeWriter, UnicodeDictWriter
from indroid.input.csv_dialects import DialectCSV
import cStringIO, re, shutil, datetime
import pymongo

db_path = r"E:\data\neo\test"
db_path = r"Q:\neo4j\embedded"

def make_custs_embedded(amount=10000, offset=0, every=10000):
    db = GraphDatabase(db_path)        
    customers = db.node[1]
    with db.transaction:
        for i in range(offset, offset+amount):
            name = 'Customer {:0=5}'.format(i)
            customer = db.node(name=name)
            customer.INSTANCE_OF(customers)
            if i % every == 0:
                print datetime.datetime.now(), i
    db.shutdown()    

def get_custs_embed(amount=-1, every=10000):
    #print len(customers.INSTANCE_OF)
    #l = customers.INSTANCE_OF
    db = GraphDatabase(db_path)    
    l = db.nodes
    for j in range(2):
        for i, node in enumerate(l):
            if amount >= 0 and i > amount:
                break
            if i % every == 0:
                print datetime.datetime.now(), node
    db.shutdown()

if __name__ == '__main__':
    make_custs_embedded(amount=50000, offset=0)
    #make_custs_rest(amount=50000)
    get_custs_rest()
    get_custs_embed()
