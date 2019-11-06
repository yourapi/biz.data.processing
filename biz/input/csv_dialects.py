from __future__ import absolute_import
import csv

class DialectCSV(csv.Dialect):
    delimiter = ';'
    doublequote = True
    escapechar = None
    lineterminator = '\n'
    quotechar = '"'
    quoting = csv.QUOTE_MINIMAL
    skipinitialspace = False

class DialectCSV_NoQuote(csv.Dialect):
    delimiter = ';'
    doublequote = False
    escapechar = None
    lineterminator = '\n'
    quotechar = ''
    quoting = csv.QUOTE_NONE
    skipinitialspace = False

class DialectCSVQ(csv.Dialect):
    delimiter = ';'
    doublequote = True
    escapechar = None
    lineterminator = '\n'
    quotechar = '"'
    quoting = csv.QUOTE_ALL
    skipinitialspace = False

class DialectComma(csv.Dialect):
    delimiter = ','
    doublequote = True
    escapechar = None
    lineterminator = '\n'
    quotechar = '"'
    quoting = csv.QUOTE_MINIMAL
    skipinitialspace = False

class DialectCommaDQ(csv.Dialect):
    delimiter = ','
    doublequote = True
    escapechar = None
    lineterminator = '\n'
    quotechar = '"'
    quoting = csv.QUOTE_ALL
    skipinitialspace = False

class DialectMySQL(csv.Dialect):
    delimiter = ','
    doublequote = True
    escapechar = '\\'
    lineterminator = '\n'
    quotechar = "'"
    quoting = csv.QUOTE_MINIMAL
    skipinitialspace = False

class DialectPipe(csv.Dialect):
    delimiter = '|'
    doublequote = True
    escapechar = None
    lineterminator = '\n'
    quotechar = '"'
    quoting = csv.QUOTE_MINIMAL
    skipinitialspace = False

class DialectPipeNoEscape(DialectPipe):
    doublequote = False
    quotechar = None
    quoting = csv.QUOTE_NONE

class DialectTab(csv.Dialect):
    delimiter = '\t'
    doublequote = True
    escapechar = None
    lineterminator = '\n'
    quotechar = '"'
    quoting = csv.QUOTE_MINIMAL
    skipinitialspace = False
    
