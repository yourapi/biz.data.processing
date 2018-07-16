"""General objects for generating data from a source."""
from collections import OrderedDict, Counter
import re

class Row(dict):
    "A container for a row from any source. A row can contain a subrow in a field to enable hierarchical data."
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            # Must raise attribute-error for correct handling of calling parties:
            raise AttributeError
    def __setattr__(self, name, value):
        if name in self.__dict__ or 'dict' in name:
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
        def standardize_fieldname(fieldname):
            fieldname = re.sub('\W', '_', fieldname)
            while '__' in fieldname:
                fieldname = fieldname.replace('__', '_')
            if re.match('\d.*', fieldname):
                fieldname = '_' + fieldname
            return fieldname
        self.name = name
        self.stream = stream
        if strip_fieldnames:
            fieldnames = [fieldname.strip() for fieldname in fieldnames]
        if standardize_fieldnames:
            fieldnames = [standardize_fieldname(fieldname) for fieldname in fieldnames]
        if lowercase_fieldnames:
            fieldnames = [fieldname.lower() for fieldname in fieldnames]
        if make_unique_fieldnames:
            fieldnames_cnt = Counter()
            for fieldname in fieldnames:
                fieldnames_cnt[fieldname.lower()] += 1
            for i in reversed(range(len(fieldnames))):
                if fieldnames_cnt[fieldnames[i].lower()] > 1:
                    fieldnames[i] = fieldnames[i] + str(fieldnames_cnt[fieldnames[i].lower()])
                    fieldnames_cnt[fieldnames[i].lower()] -= 1
        self.fieldnames = fieldnames
        self.max_rows = max_rows
    def __iter__(self):
        """Generates Row-objects until EOF. Example generator:
        for i in range(10):
            yield(Row(i=i))"""
        raise NotImplementedError()
    def rows(self):
        return [row for row in self]

class Source(object):
    def __init__(self, name=None, date=None, max_rows=None):
        self.name = name
        self.date = date
        self._tables = []
        self._table_by_name = {}
        self._index = -1
        self.max_rows = max_rows
    def tables(self):
        return self._tables
    def table(self, name_or_number):
        "Return a table by name, case-insensitive comparison!"
        if isinstance(name_or_number, basestring):
            if not self._table_by_name:
                for table in self.tables():
                    self._table_by_name[table.name.lower()] = table
            return self._table_by_name[name_or_number.lower()]
        else:
            return self._tables[name_or_number]
    def table_names(self):
        return [table.name for table in self]
    def __iter__(self):
        for table in self._tables:
            yield table

if __name__ == '__main__':
    for r in Table():
        print r