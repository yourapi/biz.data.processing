import sys, rdflib
from rdflib.graph import ConjunctiveGraph as Graph
from rdflib import plugin
from rdflib.store import Store, NO_STORE, VALID_STORE
from rdflib.namespace import Namespace
from biz.input import csv
from datetime import datetime
from biz.rdf.sesame2 import SesameGraph, URIRef, Literal

def write(filename, logfile=r"X:\code\biz\rdf\test4.txt", max_rows=None):
    repository = 'test003'
    endpoint = "http://localhost:8080/openrdf-sesame/repositories/%s" % (repository)
    
    ctx = 'file://{}'.format(filename[2:].replace('\\', '/'))
    print ctx
    g = SesameGraph(endpoint, ctx, commit_threshold=980000)
    ns = Namespace('i:')
    #g.bind('imp', ns)
    #g.namespaces['imp'] = ns
    max_rows = int(max_rows) if max_rows else None
    for i, row in enumerate(csv.Source(filename,
                                       max_rows=max_rows).table(0)):
        if i % 1000 == 0:
            line = '{} {}'.format(i, datetime.now())
            print line
            open(logfile, 'a').write(line + '\n')
        for k, v in row.items():
            g.add((URIRef(ns['row_{}'.format(i)]), URIRef(ns[k]), Literal(v)))
    
    g.commit()
    
    print "Triples in graph after add: ", len(g)

if __name__ == '__main__':
    write(*sys.argv[1:])
