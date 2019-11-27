"""When running a test from this directory, take care to remove current dir from python-path to 
prevent standard libraries like xml and csv to come in the way of the intended libraries."""
from __future__ import absolute_import
import os, sys
if os.getcwd() in sys.path:
    sys.path.remove(os.getcwd())
from indroid.input.csv_reader import UnicodeReader
from indroid.input.csv_dialects import DialectCSV
from indroid.input.general import *

class Row(Row):
    def __init__(self, row, fieldnames=None):
        super(Row, self).__init__()
        for fieldname, value in zip(fieldnames, row):
            self[fieldname] = value

class Table(Table):
    def __init__(self, reader, name, max_rows=None, fieldnames_row=None, fieldnames=[]):
        # Find the row with fieldnames, assuming it has fieldnames:
        # The fieldnames is the first completely filled row (e.g. no blanks)
        # When no fieldnames row is found in the first 100 lines, 
        len_max = 0
        if not fieldnames:
            if fieldnames_row is None:
                for i, row in enumerate(reader):
                    if all(row):
                        fieldnames = row
                        break
                    l = len([v for v in row if v])
                    if l > len_max:
                        fieldnames = row
                    if i > 100:
                        break
            else:
                for i, row in enumerate(reader):
                    if i == fieldnames_row:
                        fieldnames = row
                        break
        super(Table, self).__init__(reader, name, row, max_rows=max_rows)
        self.fieldnames = fieldnames
        self.row_offset = i
    def __iter__(self):
        if self.max_rows is None:
            for row in self.stream:
                #yield row
                yield Row(row, self.fieldnames)
        else:
            for i, row in enumerate(self.stream):
                if i >= self.max_rows:
                    break
                yield Row(row, self.fieldnames)

class Source(Source):
    def __init__(self, filename, dialect=None, encoding=None, max_rows=None, kwargs_reader={}, kwargs_tables={}):
        "The filename is stored as self.name and the path as self.path."
        self.path, name = os.path.split(filename)
        super(Source, self).__init__(name=name, max_rows=max_rows)
        reader = UnicodeReader(filename, dialect=dialect, encoding=encoding, **kwargs_reader)
        self._tables.append(Table(reader, os.path.splitext(name)[0], max_rows, **kwargs_tables))

def test():
    for src, encoding in (('-ansi', None), ('-cp1252', None), ('-utf8', None), (' - Copy', None)):
        print src, encoding, 
        source = Source(r"X:\data\source\kpn\swol_marketing\odin\2012-09-26\extract\odin_dump-packages{}.csv".format(src),
                        dialect=None, encoding=encoding)
        rows = []
        for table in source:
            print table.name, table.reader.encoding
            for row in table:
                rows.append(row)
        print rows[0].keys()[0]
        for i in (0, 1):
            print rows[i].package
        print len(rows)

if __name__ == '__main__':
    test()