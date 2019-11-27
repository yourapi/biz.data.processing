"""When running a test from this directory, take care to remove current dir from python-path to 
prevent standard libraries like xml and csv to come in the way of the intended libraries."""
from __future__ import absolute_import
import os, sys
if os.getcwd() in sys.path:
    sys.path.remove(os.getcwd())
from indroid.output.csv_writer import DictWriter
from indroid.input.csv_dialects import DialectCSV
from indroid.output.general import Row, Table, Sink

class Row(Row):
    def __init__(self, row, fieldnames=None):
        super(Row, self).__init__()
        for fieldname, value in zip(fieldnames, row):
            self[fieldname] = value

class Table(Table):
    def __init__(self, reader, name, max_rows=None):
        # Find the row with fieldnames, assuming it has fieldnames:
        for i, row in enumerate(reader):
            if all(row):
                break
        super(Table, self).__init__(reader, name, row, max_rows=max_rows)
        self.fieldnames = row
        self.row_offset = i
    def __iter__(self):
        if self.max_rows is None:
            for row in self.stream:
                yield Row(row, self.fieldnames)
        else:
            for i, row in enumerate(self.stream):
                if i >= self.max_rows:
                    break
                yield Row(row, self.fieldnames)

class Sink(Sink):
    def __init__(self, filename, dialect=None, encoding=None, max_rows=None, **kwargs):
        "The filename is stored as self.name and the path as self.path."
        self.path, name = os.path.split(filename)
        super(Source, self).__init__(name=name, max_rows=max_rows)
        reader = UnicodeReader(filename, dialect=dialect, encoding=encoding, **kwargs)
        self._tables.append(Table(reader, os.path.splitext(name)[0], max_rows))

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