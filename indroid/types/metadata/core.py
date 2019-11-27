# mapping.py - find, save and map attributes from a dataframe.
from __future__ import division
import numpy as np
from indroid import pandas as pd
import re, copy, collections, hashlib, json
from isounidecode import unidecode
from indroid.metadata.types import Types
from os.path import join, split, splitext, splitdrive, isdir, isfile, exists
from indroid.pandas.tools.container import PrimusInterPares

def clean_names(names):
    """Replace all names with names which can be used as property names,
    e.g. replacing a space with an underscore. All letters are converted to lower case:
    Last Name --> last_name
    Zip-Code ; Zip Code  --> zip_code_1 ; zip_code_2  (if make_unique is True)
    The supplied argument is a series which is returned as a clean series.
    """
    def cleaner(name):
        name = re.sub('[\s!@#$%^&\*(\)\.:";''/?\\\\|<>\[\]~\-=]', '_', unidecode(name).strip().lower())
        while '__' in name: name = name.replace('__', '_')
        if re.match('\d.*', name): name = '_' + name
        return name
    names = names.apply(cleaner)
    counts = collections.Counter(names)
    result = []
    for name in names[::-1]:
        if counts[name] > 1:
            suffix = '{}'.format(counts[name])
            if not name.endswith('_'): suffix = '_' + suffix
            while name + suffix in counts:
                suffix += 'a'
            result.insert(0, name + suffix)
            counts[name] -= 1
        else:
            result.insert(0, name)
    return pd.Series(result, name=names.name, index=names.index)

class Metadata(PrimusInterPares):
    """Store all possible information about the dataframe. Map columns to best matching type."""
    types = Types()   # Types is instantiated on the class-level, so is only instantiated once!
    def __init__(self, levels=None, data_root=None, filename='metadata.csv', default_extension='.csv'):
        super(Metadata, self).__init__()
        self._levels = self._data_root = self._default_extension = self._filename = None
        self.set(levels, data_root, filename, default_extension)
    def copy(self):
        return copy.copy(self)
    @property
    def filename(self):
        return self._filename if self._filename and exists(self._filename) else None
    def pip_id(self):
        "Unique ID for metadata."
        return self.filename
    def assume(self, other):
        "Set the defining properties fromthe other as own properties."
        assert isinstance(other, Metadata)
        self.set(other._levels, other._data_root, splitext(split(other.filename)[-1])[0])
    def set(self, levels, data_root, filename, default_extension=None):
        if levels:
            self._levels = levels
        if data_root:
            self._data_root = data_root
        if default_extension:
            self._default_extension = default_extension
        if not (self._levels and self._data_root and filename):
            return
        if not splitext(filename)[-1] and self._default_extension:
            filename += self._default_extension
        self._filename = join(self._data_root, join(*self._levels), filename)
        self.add(self)
        if exists(self._filename):
            self._data = pd.read_csv(self._filename, sep=';', encoding='cp1252')
        else:
            self._data = pd.DataFrame()
    def function_hash(self, function_name='convert'):
        """Return a hash based on the code of all present functions. All specified types are looked up
        in the type-specification and the code of the specified function is hashed. Finally, all hashes 
        are combined together in one hash and returned."""
        result = hashlib.md5()
        if 'type' in self._data:
            for typename in self._data['type'].dropna().drop_duplicates().order():
                if typename in self.types:
                    t = self.types[typename]
                    result.update(t.function_hash(function_name))
                    # Also get the hash code for the categorizers,for they might also have changed.
                    # Get the match-categorizers, and get the hash-code for all categorizers up to the highest found:
                    cats = []
                    for m in self._data[self._data['type'] == typename]['match'].dropna():
                        cats += json.loads(m).keys()
                    result.update(t.categorizer_hash(max(cats)))
        return result.hexdigest()
    def map(self, df, make_unique=True, clean=True, normalize=True, inplace=True):
        """Replace the column names with standard names based on the type of the value.
        The type is inferred from the type-selection mechanism. If multiple columns with the
        same type exist and make_unique=True, the standard names are suffixed with the
        original name of the attribute, ."""
        from indroid.pandas import clean_column_names
        def columns_match(df):
            "Do a match for best fitting type for every contained column."
            self._dirty = False
            def column_match(series):
                """Comparison of supplied series with available types. If not found, do a match and add to metadata.
                If found, do a quick check of the type."""
                def match_row(series):
                    matches = self.types.matches(series)
                    # and store the found data:
                    if matches:
                        match = matches[0]
                        row = pd.Series({'name_auto': match.type.property_name, 'type_auto': match.type.name, 
                                         'match': match.match.to_json(), 'match_mean': match.match.mean(), 
                                         'alternative_type_auto': ','.join([m.type.name for m in matches[1:]])})
                    else:
                        row = pd.Series({'name':'', 'type': '', 'name_auto': '', 'type_auto': '',
                                         'match': '', 'match_mean': '', 'alternative_type_auto': ''})
                    row['column_name'] = column_name
                    row['sample_values'] = ','.join(['{} ({})'.format(*i) for i in series.dropna().value_counts()[:5].iteritems()])
                    row['name_manual'] = ''
                    row['type_manual'] = ''
                    self._dirty = True
                    return row
                column_name = series.name
                # ToDo: check if column name is in _data!!!!
                row = pd.Series(self._data[self._data.column_name == column_name].iloc[0]) \
                    if 'column_name' in self._data and (self._data.column_name == column_name).any() \
                    else pd.Series()
                if not row.empty:
                    # Found, get data and do a check:
                    # Set auto-name if manual type specified
                    # ToDo: manualoverride of str-type does not work yet
                    type_manual = row.dropna().get('type_manual')
                    if type_manual and type_manual in self.types and row['name_auto'] != self.types[type_manual].property_name:
                        row['name_auto'] = self.types[type_manual].property_name
                        #self._dirty = True
                    elif row.get('type') and row.get('type') is not np.nan and not self.types.verify(row.get('type'), series):
                        row = match_row(series)
                    # ToDo, save the corpus in an appropriate place if new type specified.
                else:
                    # Not found, do a match:
                    row = match_row(series)
                # Compute the definitive type and typename:
                row['name'] = ''
                row['type'] = ''
                for key, keys in (('name', ['name_manual', 'name_auto']),
                                  ('type', ['type_manual', 'type_auto'])):
                    if all([k in row for k in keys]) and not row[keys].empty and not row[keys].dropna().empty:
                        for k in keys:
                            if row.dropna().get(k):
                                row[key] = row[k]
                                break
                return row
            result = pd.DataFrame([column_match(df.icol(i)) for i in range(df.shape[1])])
            # Save the data if necessary:
            return result
        # ToDo: map all columns, set the new names for the columns, convert the data types.
        if not inplace: df = df.copy()
        matches = columns_match(df)
        # Get the previous column_name_new, if any, for comparison with new column names:
        names_new = matches.column_name_new if 'column_name_new' in matches else pd.Series()
        # Now determine the new names. Set the names in three stages: - no match  - single match  - multiple match
        no_match = matches.name.fillna('') == ''
        count_by_name = matches.groupby('name').size()
        single_match = matches.name.isin(count_by_name[count_by_name == 1].index)
        multi_match = matches.name.isin(count_by_name[count_by_name > 1].index)
        matches.loc[no_match, 'column_name_new'] =  matches.loc[no_match, 'column_name']
        matches.loc[single_match, 'column_name_new'] =  matches.loc[single_match, 'name']
        matches.loc[multi_match, 'column_name_new'] =  matches.loc[multi_match, 'name'] + '_' + matches.loc[multi_match, 'column_name']
        matches['column_name_new'] = clean_names(matches.column_name_new)
        # Save the complete data, so including unused columns:
        if self._dirty or (len(names_new) != len(matches.column_name_new)) or (names_new != matches.column_name_new).any():
            cond = ~self._data.column_name.isin(matches.column_name) if 'column_name' in self._data and 'column_name' in matches else slice(0, None)
            self._data = pd.concat([matches, self._data[cond]])
            self._data.to_csv(self._filename, cols='column_name column_name_new name type name_auto type_auto match match_mean sample_values alternative_type_auto name_manual type_manual'.split())
            self._dirty = False
        # Now construct a dataframe with the new columns: converted if appropriate, otherwise the original column.
        # Make a new dataframe because in-datafraame changes clash with the current type of the column.
        # Use an ordereddict to preserve order of columns:
        new_columns = collections.OrderedDict()
        for i, name in zip(range(df.shape[1]), matches.column_name_new.values):
            t = matches.iloc[i]['type']            
            convert = self.types[t].convert if t in self.types and hasattr(self.types[t], 'convert') else None
            new_columns[name] = df.iloc[:,i].apply(convert) if convert else df.iloc[:,i]
        # Add metadata to the newly constructed dataframe:
        return pd.DataFrame(new_columns).assume_metadata(df)

if __name__ == '__main__':
    pass