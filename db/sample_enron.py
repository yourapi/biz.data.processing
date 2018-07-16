#!/usr/bin/python -i

####################################
##          Initialize            ##
####################################

import sys, math, operator, gevent
from gevent.queue import JoinableQueue, Queue, Empty
from collections import Counter, defaultdict

import fdb, fdb.tuple

fdb.api_version(22)

db = fdb.open(event_model="gevent")

####################################
##         Data Modeling          ##
####################################

# Sub-keyspaces

@fdb.transactional
def clearSubKeyspace(tr, prefix):
    tr.clear_range_startswith(fdb.tuple.pack( (prefix,) ))

# Attributes

@fdb.transactional
def addAttributeValue( tr, entityID, attribute, value ):
    tr[ fdb.tuple.pack( ("ER", entityID, attribute) ) ] = str(value)

@fdb.transactional
def getAttributeValue( tr, entityID, attribute ):
    return tr[ fdb.tuple.pack( ("ER", entityID, attribute) ) ]

# Relationships

@fdb.transactional
def addRelationship( tr, relationship, primaryKey, foreignKey ):
    tr[ fdb.tuple.pack( ("ER", relationship, primaryKey, foreignKey) ) ] = ""

@fdb.transactional
def getRelationships( tr, relationship ):
    return [ fdb.tuple.unpack(k)[2:]
            for k,v in tr.get_range_startswith(fdb.tuple.pack( ("ER", relationship ) ),
                                            streaming_mode = fdb.StreamingMode.want_all ) ]

@fdb.transactional
def getRelatedEntities( tr, relationship, primaryKey ):
    items = tr[ fdb.tuple.range( ("ER", relationship, primaryKey ) ) ]
    return [ fdb.tuple.unpack(k)[-1] for k,v in items ]

@fdb.transactional
def isRelatedEntity( tr, relationship, primaryKey, foreignKey ):
    return tr[ fdb.tuple.pack( ("ER", relationship, primaryKey, foreignKey ) ) ].present()

####################################
##        Loading the Data        ##
####################################

# Location of your downloaded data set
localdir = '/Users/stephenpimentel/Enron/'

# Code available on-line at URL in document
from email_util import EmailWalker

# Initial data model:
# Treat email addresses as unique IDs for persons
# Store persons and emails as entities
# Store all fields that have persons as values as relationships, i.e.,
#   sender:, to:, cc:, bcc:
# Store all other fields as attributes of email entity

# recipientFields are tuples when non-null, but empty list when null
recipientFields = ['to','cc','bcc']
attributeFields = ['folder','sendername','subject','date','text']

@fdb.transactional
def addEmail( tr, email ):
    # Check if the email has already been added to enforce idempotence
    if isRelatedEntity(tr,'sender', email['sender'], email['mid']): return
    # Add relationship for 'sender'
    addRelationship( tr, 'sender', email['sender'], email['mid'])
    # Add relationships for each recipient field
    for field in recipientFields:
        for recipient in email[field]:
            addRelationship( tr, field, email['mid'], recipient)
    # Add attributes for each attribute field
    for field in attributeFields:
        if email[field]:
            fieldValue = str(email[field])[:40000]
            addAttributeValue( tr, email['mid'], field, fieldValue )

def loadEnronEmailsSequential():
    clearSubKeyspace(db,'ER')
    walker = EmailWalker(localdir)
    for email in walker: addEmail(db, email)

class BoundedBuffer(Queue):
    def __init__(self, reader, writer, number_consumers=50):
        # Setting maxsize to the number of consumers will make producers
        # wait to put a task in the queue until some consumer is free
        super(BoundedBuffer, self).__init__(maxsize=number_consumers)
        self._number_consumers = number_consumers
        self._reader = reader
        self._writer = writer

    def _producer(self):
        # put will block if maxsize of queue is reached
        for data in self._reader: self.put(data)

    def _consumer(self):
        try:
            while True:
                data = self.get(block=False)
                self._writer(db, data)
                gevent.sleep(0) # yield
        except Empty: pass

    def produce_and_consume(self):
        producers = [gevent.spawn(self._producer)]
        consumers = [gevent.spawn(self._consumer) for _ in xrange(self._number_consumers)]
        gevent.joinall(producers)
        gevent.joinall(consumers)

def loadEnronEmails(folder=''):
    clearSubKeyspace(db,'ER')
    reader = EmailWalker(localdir+folder)
    tasks = BoundedBuffer(reader, addEmail)
    tasks.produce_and_consume()

####################################
##        Social Graph            ##
####################################

def indexContacts():
    db.clear_range_startswith(fdb.tuple.pack( ('contact',) ))
    for sender, emailID in getRelationships( db, 'sender'):
        for recipient in getRelatedEntities(db, 'to', emailID):
            addContact( db, sender, recipient)

@fdb.transactional
def addContact( tr, sender, recipient ):
    weight = tr[ fdb.tuple.pack( ('contact', sender, recipient) ) ]
    tr[ fdb.tuple.pack( ('contact', sender, recipient) ) ] = str(int(weight or 0)+1)

@fdb.transactional
def getContacts( tr, sender ):
    return [ fdb.tuple.unpack(k)[-1] for k,_ in tr[ fdb.tuple.range( ('contact', sender))]]

@fdb.transactional
def getAllContacts( tr ):
    return [ fdb.tuple.unpack(k)[1:]
         for k,_ in tr.get_range_startswith(fdb.tuple.pack( ('contact',) ),
                                     streaming_mode = fdb.StreamingMode.want_all ) ]

# How many messages has sender sent to recipient?
# Version using the ER model requires client to perform join
@fdb.transactional
def countMessagesWithJoin( tr, sender, recipient ):
    return len([emailID for emailID in getRelatedEntities( db, 'sender', sender)
                if isRelatedEntity(tr, 'to', emailID, recipient)])

@fdb.transactional
def countMessages(tr, sender, recipient ):
    return int(tr[ fdb.tuple.pack( ('contact', sender, recipient) ) ] or 0)

@fdb.transactional
def sortContacts( tr ):
    KVPairs = tr.get_range_startswith(fdb.tuple.pack( ('contact',) ),
                                    streaming_mode = fdb.StreamingMode.want_all )
    contacts = [ fdb.tuple.unpack(k)[1:]+(int(v),) for k,v in KVPairs ]
    return sorted(contacts, key=operator.itemgetter(2), reverse=True)

# Extended Contacts

def indexSecondDegree():
    db.clear_range_startswith(fdb.tuple.pack( ('second',) ))
    for sender, recipient in getAllContacts(db):
        for second in getContacts( db, recipient):
            db[ fdb.tuple.pack( ('second', sender, second) ) ] = ''

@fdb.transactional
def getSecondDegrees( tr, sender ):
    return [ fdb.tuple.unpack(k)[-1] for k,_ in tr[ fdb.tuple.range( ('second',sender) )]]

def getExtendedContacts( sender, degree ):
    if degree == 1:
        return getContacts( db, sender )
    elif degree == 2:
        return list(set(getContacts(db, sender) + getSecondDegrees( db, sender)))
    else:
        contacts = []
        for c in getContacts( db, sender):
            contacts.append(c)
            contacts = contacts + getExtendedContacts( c, degree-1)
        return list(set(contacts))

####################################
##      Concurrent Traversal      ##
####################################

class BoundedTraversal(JoinableQueue):
    def __init__(self, get_children, number_workers=50):
        # Setting maxsize to the number of workers will block
        # putting a node in the queue until some worker is free
        super(BoundedTraversal, self).__init__(maxsize=number_workers)
        self._get_children = get_children
        self._number_workers = number_workers
        self._distance = {}

    def _worker(self, maxdistance):
        while True:
            node = self.get()
            if self._distance[node] < maxdistance:
                try:
                    # Range read will usually block the greenlet
                    children = self._get_children(db, node)
                    self._check_children(children,self._distance[node])
                finally: self.task_done()

    def _check_children(self, children, parent_distance):
        for c in children:
            # If c is unvisited or only visited via a longer path, then visit it
            if (c not in self._distance) or (self._distance[c] > parent_distance + 1):
                self._distance[c] = parent_distance + 1
                # put will block when maxsize of queue is reached
                self.put(c)

    def run_workers(self, root, maxdistance):
        self._distance[root] = 0
        children = self._get_children(db, root)
        gevent.spawn(self._check_children, children, 0)
        workers = [gevent.spawn(self._worker, maxdistance)
                    for _ in xrange(self._number_workers)]
        self.join
        gevent.joinall(workers,timeout=1)

    def visited(self):
        return self._distance.keys()

def getBoundedContacts(sender, degree):
    bounded = BoundedTraversal(getContacts, 50)
    bounded.run_workers(sender, degree)
    return bounded.visited()

####################################
##             TF-IDF             ##
####################################

# Code available on-line at URL in document
from get_terms import *

@fdb.transactional
def index_score(tr, score_tuple):
    docType, score, docID, term = score_tuple
    tr[ fdb.tuple.pack( (docType+'_term', docID, score, term) ) ] = ""
    tr[ fdb.tuple.pack( ('term_'+docType, term, score, docID) ) ] = ""

# The docType parameter defines the abstract "documents" to be used for TF-IDF
# For example,
#   docType = 'sender' would use person sending email
#   docType = 'mid' would use email itself

def tfidf(docType):

    if docType not in ['mid', 'sender']: raise Exception('document type unknown')

    document_tf = defaultdict(Counter) # document_tf = { docID : Counter( { term : tf })}
    terms_per_document = defaultdict(set)

    for sender, emailID in getRelationships( db, 'sender'):

        if docType == 'mid': docID = emailID
        elif docType == 'sender': docID = sender

        subject = getAttributeValue(db, emailID, 'subject') or ''
        text = getAttributeValue(db, emailID,'text') or ''
        # Tokenize and cleanse the text
        terms_in_email = get_terms(subject) + get_terms(text)
        # Retrieve Counter for document and increment it for each term
        document_tf[docID].update(terms_in_email)
        # Collect all of the terms in each document
        terms_per_document[docID].update(terms_in_email)

    # allterms maps each term to the number of documents it occurs in
    allterms = Counter()
    for _, terms in terms_per_document.iteritems():
        # Increment the counter value for each term in `terms`
        allterms.update(terms)

    # Compute the Inverse Document Frequencies
    idfs = {}
    # Get the number of documents from the number of keys
    ndocuments = len(terms_per_document)
    for term, count in allterms.iteritems():
        # idfs = { term : frequency }
        idfs[term] = math.log( float(ndocuments) / float(count) )

    # Combine the Term Frequencies and Inverse Document Frequencies into TF-IDFs
    # Convert the TF-IDFs to integers with digits sufficient to preserve order
    # Return a generator of tuples with TD-IDFs to be indexed
    return ((docType, int(round(tf * idfs[term]*100)), docID, term)
            for docID, tfs in document_tf.iteritems()
            for term, tf in tfs.iteritems())

def indexEmailTerms(docType):
    clearSubKeyspace(db, docType+'_term')
    clearSubKeyspace(db, 'term_'+docType)
    reader = tfidf(docType)
    tasks = BoundedBuffer(reader, index_score)
    tasks.produce_and_consume()

####################################
##   Query Functions for TF-IDF   ##
####################################

@fdb.transactional
def getTermsForSender(tr, n, sender):
    prefix = fdb.tuple.pack( ('sender_term', sender) )
    return [ fdb.tuple.unpack(k)[-1]
             for k, _ in tr.get_range_startswith(prefix, n, reverse=True) ]

@fdb.transactional
def getTermsForMessage(tr, n, emailID):
    prefix = fdb.tuple.pack( ('mid_term', emailID) )
    return [ fdb.tuple.unpack(k)[-1]
             for k, _ in tr.get_range_startswith(prefix, n, reverse=True) ]

@fdb.transactional
def getSendersForTerm(tr, n, term):
    prefix = fdb.tuple.pack( ('term_sender', term) )
    return [ fdb.tuple.unpack(k)[-1]
             for k, _ in tr.get_range_startswith(prefix, n, reverse=True) ]

@fdb.transactional
def getMessagesForTerm(tr, n, term):
    prefix = fdb.tuple.pack( ('term_mid', term) )
    return [ fdb.tuple.unpack(k)[-1]
             for k, _ in tr.get_range_startswith(prefix, n, reverse=True) ]