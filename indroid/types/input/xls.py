"""Get data from Excel file, xls or xlsx. Find the header names heuristically, ignoring extra rows at the beginning."""
import os, sys
if os.getcwd() in sys.path:
    sys.path.remove(os.getcwd())
from xlrd import open_workbook
from xlrd.sheet import Sheet
from xlrd.xldate import xldate_as_tuple
from datetime import datetime
from indroid.input.general import *

class Row(Row):
    def __init__(self, row, fieldnames=None, book=None):
        super(Row, self).__init__()
        for fieldname, cell in zip(fieldnames, row):
            if cell.ctype == 3:
                self[fieldname] = datetime(*xldate_as_tuple(cell.value, book.datemode))
            else:
                self[fieldname] = cell.value

class Table(Table):
    def __init__(self, sheet, max_rows=None):
        assert isinstance(sheet, Sheet)
        self.sheet = sheet
        self.name = sheet.name
        self.max_rows = max_rows
        # Find the row with fieldnames, assuming it has fieldnames:
        for rowx in range(sheet.nrows):
            if all([sheet.cell(rowx, colx).value for colx in range(sheet.ncols)]):
                break
        self.fieldnames = [str(sheet.cell(rowx, colx).value) for colx in range(sheet.ncols)]
        self.row_offset = rowx + 1
    def __iter__(self):
        "Return the next row with fieldnames. as a Row-object."
        row_cnt = self.sheet.nrows if self.max_rows is None else self.max_rows + self.row_offset
        if row_cnt > self.sheet.nrows:
            row_cnt = self.sheet.nrows
        for i in range(self.row_offset, row_cnt):
            yield Row(self.sheet.row(i), self.fieldnames, self.sheet.book)

class Source(Source):
    def __init__(self, filename,max_rows=None):
        "The filename is stored as self.name and the path as self.path."
        self.path, name = os.path.split(filename)
        super(Source, self).__init__(name, max_rows=max_rows)
        wb = open_workbook(filename)
        for sheet in wb.sheets():
            self._tables.append(Table(sheet, max_rows))

def test():
    source = Source(r"X:\data\source\kpn\swol_marketing\odin\2012-09-26\extract\odin_dump-packages.xls")
    rows = []
    for table in source:
        for row in table:
            rows.append(row)
    print len(rows)
    for i in (0, 285, 284, 569):
        print rows[i]    

if __name__ == '__main__':
    test()