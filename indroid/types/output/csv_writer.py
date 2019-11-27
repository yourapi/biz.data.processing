#!/usr/bin/env python

"""Module for reading csv-like files. A number of classes are present for types and typeless (string-only)
generation of data.
ToDo: generate Unicode-values only and correctly handle input-files """
from __future__ import absolute_import  # Make sure that system-modules are loaded by default
import os, sys
if os.getcwd() in sys.path:
    sys.path.remove(os.getcwd())  # Do not import local csv-module by accident!
import csv, codecs, cStringIO, chardet
from collections import OrderedDict
from indroid.input.csv_dialects import *

class UnicodeWriterOrg:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        #data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

class Writer(object):
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding. Input is always unicode!
    """

    def __init__(self, file_or_filename, dialect=DialectCSV, encoding="utf-8", create_path=True, mode='w', **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        if isinstance(file_or_filename, file):
            self.stream = file_or_filename 
        else:
            if create_path:
                try:
                    os.makedirs(os.path.split(file_or_filename)[0])
                except:
                    pass
            self.stream = codecs.open(file_or_filename, mode=mode, encoding=encoding)        
        if 'a' not in mode:
            bom = codecs.BOM_UTF8 if encoding == 'utf-8' else codecs.BOM_UTF16 if encoding == 'utf-16' else \
                codecs.BOM_UTF32  if encoding == 'utf-32' else ''
            self.stream.stream.write(bom)
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([(s if isinstance(s, basestring) else str(s)).encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        #data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)
    def close(self):
        self.stream.close()

class DictWriter(Writer):
    def __init__(self, file_or_filename, fieldnames=[], write_header=True, restval='', dialect=DialectCSV, encoding='utf-8', create_path=True, mode='w', **kwds):
        super(DictWriter, self).__init__(file_or_filename, dialect, encoding, create_path, mode, **kwds)
        self.fieldnames = fieldnames
        if write_header and fieldnames and 'a' not in mode:
            row = {}
            for fieldname in fieldnames:
                row[fieldname] = fieldname
            self.writerow(row)
    def writerow(self, row):
        super(DictWriter, self).writerow([row[k] if k in row else u'' for k in self.fieldnames])
