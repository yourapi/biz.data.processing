"""Get data from Excel 2007-2010 file, xlsx. Find the header names heuristically, ignoring extra rows at the beginning.
For reading Excel 1997-2003, use the xls_-module."""
import os, sys
if os.getcwd() in sys.path:
    sys.path.remove(os.getcwd())
import openpyxl as xls
from datetime import datetime
from indroid.input.general import *

class Row(Row):
    def __init__(self, row, fieldnames=None):
        super(Row, self).__init__()
        for fieldname, cell in zip(fieldnames, row):
            self[fieldname] = cell.value

class Table(Table):
    def __init__(self, sheet):
        assert isinstance(sheet, xls.worksheet.Worksheet)
        self.sheet = sheet
        self.name = sheet.title
        # Find the row with fieldnames, assuming it has fieldnames:
        for i, row in enumerate(sheet.rows):
            if all([cell.value for cell in row]):
                break
        self.fieldnames = [str(cell.value) for cell in row]
        self.row_offset = i
    def __iter__(self):
        for i, row in enumerate(self.sheet.rows):
            if i <= self.row_offset:
                continue
            yield Row(row, self.fieldnames)

class Source(Source):
    def __init__(self, filename):
        "The filename is stored as self.name and the path as self.path."
        self.path, name = os.path.split(filename)
        super(Source, self).__init__(name)
        wb = xls.load_workbook(filename)
        for sheet in wb.worksheets:
            self._tables.append(Table(sheet))

def test():
    source = Source(r"X:\data\source\kpn\swol_marketing\odin\2012-09-26\extract\odin_dump-packages.xlsx")
    rows = []
    for table in source:
        for row in table:
            rows.append(row)
    print len(rows)
    for i in (0, 284, 285, 569):
        print rows[i]

if __name__ == '__main__':
    test()