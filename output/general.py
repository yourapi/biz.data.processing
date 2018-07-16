"""General objects for utputting data to a sink."""
from collections import OrderedDict, Counter
import re

class Row(OrderedDict):
    "A container for a row from any source. A row can contain a subrow in a field to enable hierarchical data."
    def __getattr__(self, name):
        try:
            return self[name]
        except:
            raise AttributeError
    def __setattr__(self, name, value):
        if name in self.__dict__ or 'OrderedDict' in name:
            super(Row, self).__setattr__(name, value)
        else:
            self[name] = value

class Table(object):
    def __init__(self, stream, name=None, fieldnames=[], strip_fieldnames=True, 
                 standardize_fieldnames=False, lowercase_fieldnames=False, 
                 make_unique_fieldnames=True, max_rows=None):
        """strip_fieldnames specifies whther leading and trailing spaces should be removed. 
        When standardize_fieldnames is specified, all fieldnames are lower-cased, every non 
        alphanum is replaced by an onderscore; when a fieldname starts with a digit, it is 
        preceded by an underscore to enable using fieldname as property-name in Python."""
        self.fieldnames = fieldnames
        self.max_rows = max_rows
    def __iter__(self):
        """Generates Row-objects until EOF. Example generator:
        for i in range(10):
            yield(Row(i=i))"""
        raise NotImplementedError()
    def rows(self):
        return [row for row in self]

class Sink(object):
    """General sink-object for outputting row-like data. Can be subclassed to enable 
    writing to csv, excel etc."""
    def __init__(self, name=None, date=None, max_rows=None):
        self.name = name
        self.date = date
        self._tables = []
        self._index = -1
        self.max_rows = max_rows
    def tables(self):
        return self._tables
    def table(self, name_or_number):
        return self._tables[name_or_number]
    def table_names(self):
        return [table.name for table in self]
    def __iter__(self):
        for table in self._tables:
            yield table

if __name__ == '__main__':
    for r in Table():
        print r