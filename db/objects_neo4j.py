from __future__ import absolute_import
import os, sys
if os.getcwd() in sys.path:
    sys.path.remove(os.getcwd())
import biz.input.csv as csv
import biz.input.xml as xml
from biz.input.csv_reader import UnicodeWriter, UnicodeDictWriter
from biz.input.csv_dialects import DialectCSV
import cStringIO, re, shutil, datetime

from neo4j import GraphDatabase, INCOMING, Evaluation
from bulbs.neo4jserver import Graph

db_path = r"E:\data\neo\test"
db_path = r"Q:\neo4j\embedded"
# Make pristine DB:
#try:
    #if os.path.exists(db_path):
        #shutil.rmtree(db_path)
    #os.makedirs(db_path)
#except:
    #pass

# Create a database
g = Graph()

# All write operations happen in a transaction
#with db.transaction:
 
    ## A node to connect customers to
    #customers = db.node()
 
    ## A node to connect invoices to
    #invoices = db.node()
 
    ## Connected to the reference node, so
    ## that we can always find them.
    #db.reference_node.CUSTOMERS(customers)
    #db.reference_node.INVOICES(invoices)
 
    ## An index, helps us rapidly look up customers
    #customer_idx = db.node.indexes.create('customers')
    
def create_customer(name):
    with db.transaction:
        customer = db.node(name=name)
        customer.INSTANCE_OF(customers)
 
        # Index the customer by name
        customer_idx['name'][name] = customer
    return customer
 
def create_invoice(customer, amount):
    with db.transaction:
        invoice = db.node(amount=amount)
        invoice.INSTANCE_OF(invoices)
 
        invoice.SENT_TO(customer)
    return customer

def get_customer(name):
    return customer_idx['name'][name].single

def get_invoices_with_amount_over(customer, min_sum):
    # Find all invoices over a given sum for a given customer.
    # Note that we return an iterator over the "invoice" column
    # in the result (['invoice']).
    #return db.query('''START customer=node({customer_id})
                       #MATCH invoice-[:SENT_TO]->customer
                       #WHERE has(invoice.amount) AND invoice.amount >= {min_sum}
                       #RETURN invoice''',
                       #customer_id = customer.id, min_sum = min_sum)['invoice']
    return db.query('''START customer=node({customer_id})
                       MATCH invoice-[:SENT_TO]->customer
                       WHERE invoice.amount >= {min_sum}
                       RETURN invoice''',
                       customer_id = customer.id, min_sum = min_sum)['invoice']

 
# Finding large invoices
#large_invoices = get_invoices_with_amount_over(get_customer('Acme Inc.'), 500)

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

def make_custs_rest(amount=10000, offset=0):
    for i in range(offset, offset+amount):
        name = 'Customer {:0=5}'.format(i)
        customer = g.vertices.create(name=name)
        if i % 1000 == 0:
            print datetime.datetime.now(), i

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

def get_custs_rest(amount=-1, every=10000):
    for j in range(2):
        print datetime.datetime.now()
        for i, v in enumerate(g.V):
            if amount >= 0 and i >= amount:
                break
            if i % every == 0:
                print datetime.datetime.now(), v

if __name__ == '__main__':
    #make_custs(amount=50000, offset=0)
    #make_custs_rest(amount=50000)
    get_custs_rest()
    get_custs_embed()
