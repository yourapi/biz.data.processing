import re, datetime, neo4j, logging
from bizold.input.file import StringReader, StringWriter
log = logging.getLogger(__name__)
#from neo4j import GraphDatabase, INCOMING, Evaluation
from bulbs.neo4jserver import Graph

g = Graph()

#db = GraphDatabase(r"Q:\neo4j\test") # Only embedded; doesn't scale well!!

def test_init():
    with db.transaction:
        # A node to connect customers to
        customers = db.node()
        # A node to connect invoices to
        invoices = db.node()
        # Connected to the reference node, so
        # that we can always find them.
        db.reference_node.CUSTOMERS(customers)
        # An index, helps us rapidly look up customers
        if not db.node.indexes.exists('customers'):
            customer_idx = db.node.indexes.create('customers')    
    print customers

def test_read(file_in=r"P:\data\source\kpn\swol_marketing\odin\2012-11-14\extract\odin_dump-small.csv"):
    prev = datetime.datetime.now()
    cnt, total = 1000, 10000
    f_in = StringReader(file_in)
    i = 0
    customers = db.reference_node.CUSTOMERS
    try:
        while True:
            with db.transaction:
                while True:
                    row = f_in.next()
                    i += 1
                    if i % cnt == 0:
                        print i, datetime.datetime.now(), (datetime.datetime.now() - prev).total_seconds() * 1000 / cnt
                        prev = datetime.datetime.now()
                        break
                    try:
                        for key in row:
                            try:
                                row[key] = unicode(row[key])
                            except:
                                row[key] = 'Geen unicode'
                            if row[key].upper() == 'NULL':
                                row[key] = ''
                        node = db.node()
                        node['name'] = row['org_name']
                    except Exception as e:
                        print i, e
                    if i >= total:
                        break
    except StopIteration:
        pass

def test_write(file_out=r"P:\data\source\kpn\swol_marketing\odin\2012-11-14\extract\export\dump.csv"):
    def lst(gen):
        if not gen:
            return ''
        return list(gen)
    f_out = StringWriter(file_out, fieldnames=['type', 'node', 'inV', 'outV', 'inE', 'outE'])
    for C in node_types:
        try:
            for node in getattr(graph, C.element_type).get_all():
                f_out.writerow({'type': C.element_type, 'node': node, 'inV': lst(node.inV()), 'outV': lst(node.outV()), 'inE': lst(node.inE()), 'outE': lst(node.outE())})
        except Exception as e:
            print e
    f_out.close()

def test_1():
    r = range(1000)
    while True:
        for i in r:
            if i % 100 == 0:
                break
        print i
        break
    print i

if __name__ == '__main__':
    #test_init()
    test_read()