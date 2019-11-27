from indroid import pandas as pd
import numpy as np
import re
import os
import csv
from dateutil.parser import parse
from collections import OrderedDict, defaultdict
from os.path import join, split, splitext
import csv, codecs, cStringIO, chardet
from chardet.universaldetector import UniversalDetector
from indroid.input.csv_dialects import *
from indroid.pandas import ExcelFile
import indroid.input.xml as bix
from indroid.pandas.core.frame import DataFrame

###############################################################################
# This module contains helper-functions for reading data, for auto-sensing dialect etc
# for reading xml and for reading sheets from an Excel-file.
# When multiple dataframes exist (like in an excel or xml-file), they are returned in a dictionary.
# When only one dataframe exists, this is always returned withou a dictionary.
###############################################################################

def read_excel(filepath_or_buffer, sheetname=None, always_asdict=False, **kwargs):
    """Return a specific sheet or all sheets in a dictionary with the sheetname as key.
    When only one (valid) sheet exists, only this one is returned, even if the name was not specified.
    If always_asdict=True, frames are always returned as a dict.
    It is up to the caller to distinguish between these cases."""
    if sheetname:
        return pd.read_excel(filepath_or_buffer, sheetname)
    else:
        result = {}
        for sheetname in ExcelFile(filepath_or_buffer).sheet_names:
            try:
                result[sheetname] = pd.read_excel(filepath_or_buffer, sheetname)
            except IndexError:
                pass
        return result.values()[0] if len(result) == 1 and not always_asdict else result or None

def read_xml(filepath_or_buffer=None, xml=None, elementname=None, always_asdict=False, **kwargs):
    """read all recurring entries and return them as a DataFrame. If multiple entries exist, return 
    multiple entries as a tuple. When only one (valid) elementname exists, only this one is returned, 
    even if the name was not specified. If always_asdict=True, frames are always returned as a dict.
    It is up to the caller to distinguish between these cases."""
    def makeframe(element):
        if isinstance(element, (list, bix.Table)):
            return DataFrame([makeframe(row) for row in element])
        if isinstance(element, bix.Row):
            return {k: makeframe(v) for k, v in element.iteritems()}
        return element  
    source = bix.Source(file_or_filename=filepath_or_buffer, xml=xml, **kwargs)
    if elementname:
        return makeframe(source.table(elementname))
    else:
        result = {table.name: makeframe(table) for table in source}
        return result.values()[0] if len(result) == 1 and not always_asdict else result or None

def read_mysql(filepath_or_buffer, tablename=None, always_asdict=False, **kwargs):
    """Read a backup-file from MySQL. Get the table-definitions and the content of each table.
    Return a specific table or all tables in a dictionary with the sheetname as key.
    When only one (valid) table exists, only this one is returned, even if the name was not specified.
    If always_asdict=True, frames are always returned as a dict.
    It is up to the caller to distinguish between these cases."""
    re_table = 'CREATE TABLE `(.+)`.*'
    re_table_end = '\s*\);\s*$'
    re_fielddef = '\s*`(.+)`\s*(\w+).*,'
    re_insert = 'INSERT INTO `(.+)` VALUES \((.*)\)'
    def table(firstline, f):
        """Get the table definition from the supplied file. The file is read until 
        the definition is ready. Return a tuple with the table name and an ordered 
        dict with the fieldnames as keys and the python types as values."""
        fields = OrderedDict()
        r = re.match(re_table, firstline, re.I)
        tablename = r.group(1) if r else None
        # Now get the field definitions:
        for line in f:
            if re.match(re_table_end, line):
                break
            r = re.match(re_fielddef, line)
            if r:
                fieldname, fieldtype = r.groups()
                # Fieldtypes are very limited. Default is str (no extra parsing), int and long 
                types = {'int': long, 'tiyint': int, 'float': float,
                         'date': lambda v: parse(v).date(),'timestamp': parse}
                fields[fieldname] = types.get(fieldtype, str)
        return tablename, fields
    if tablename:
        # ToDo
        pass
    if not isinstance(filepath_or_buffer, file):
        filepath_or_buffer = open(filepath_or_buffer)
    tables = defaultdict(list)
    tabledef = {}
    # Get the definitions and the data from the import file:
    for line in filepath_or_buffer:
        if re.match(re_table, line):
            k, v = table(line, filepath_or_buffer)
            tabledef[k] = v
        r = re.match(re_insert, line)
        if r:
            tablename, line = r.groups()
            tables[tablename].append(line)
    # The data is now in the tables. Convert using a dictreader and the MySQL-dialect:
    for tablename in tables.keys():
        tables[tablename] = [row for row in \
                             csv.DictReader(tables[tablename], fieldnames=tabledef[tablename], 
                                            dialect=DialectMySQL)]
        # Now convert to dataframes:
        tables[tablename] = pd.DataFrame(tables[tablename], columns=tabledef[tablename].keys())
        for col_name, converter in tabledef[tablename].iteritems():
            def conv(value):
                try:
                    return converter(value)
                except (ValueError, TypeError):
                    return np.nan
            # Now switch between valid values and invalid ones (which will get NaN):
            null = tables[tablename][col_name].isin(('NULL', '', '0000-00-00'))
            tables[tablename].loc[~null, col_name] = tables[tablename].loc[~null, col_name].apply(conv)
            tables[tablename].loc[null, col_name] = np.nan
    return tables

def read_csv(filepath_or_buffer, sep=None, dialect=None, encoding=None, doublequote=None, escapechar=None, 
             lineterminator=None, quotechar=None, quoting=None, skipinitialspace=None, header='infer', 
             names=None, skiprows=None, skipfooter=None, warn_bad_lines=True, error_bad_lines=True, nrows=None, dtype=str,
             encoding_threshold=.5, encoding_preferences=('1252', '8859-1'), 
             encoding_excludes=('koi', 'mik', 'iscii', 'tscii', 'viscii','jis','gb', 'big5', 'hkscs','ks', 'euc', '2022'),
             encoding_default='ascii', error_no_encoding=False, dialect_threshold=.7, sep_preferences=';,\t|', 
             **kwargs):
    """The read_csv sits on top of the pandas.io.parsers.read_csv and has some convenience functions, like a 
    better separator-sniffer and an encoding-sniffer. 
    The dialect-keyword overrules the separate dialect-parts, if both sep and dialect are None, the dialect is 
    heuristically inferred from the source. The separate dialect-keywords (doublequote, escapechar, lineterminator, 
    quotechar, quoting, skipinitialspace) are only used specifically when specified, otherwise are taken from defaults.
    If encoding is None, the encoding is inferred from the source, with a particular order which is relevant for NL but 
    may be different for other countries like DE and BE.
    The most frequently used keywords are provided, the less frequently used are passed in the **kwargs."""
    stream = open(filepath_or_buffer, 'rb') if isinstance(filepath_or_buffer, basestring) else filepath_or_buffer
    try:
        filepos_current = stream.tell()
    except:
        filepos_current = None
    if encoding is None:
        stream.seek(0)
        encoding, _ = _encoding_heuristic(stream, confidence_threshold=encoding_threshold, 
                                          encoding_preferences=encoding_preferences, encoding_excludes=encoding_excludes,
                                          encoding_default=encoding_default, error_no_encoding=error_no_encoding)
    if sep is None:
        # Get the separator:
        stream.seek(0)
        sep, _ = _sep_heuristic(stream, confidence_threshold=dialect_threshold, seps=sep_preferences)
    # ToDo: Other dialect-parameters should be derived from the source; for now,give hard-coded pandas-defaults:
    doublequote = True if doublequote is None else doublequote
    quotechar = '"' if quotechar is None else quotechar
    quoting = csv.QUOTE_MINIMAL if quoting is None else quoting
    skipinitialspace = False if skipinitialspace is None else skipinitialspace
    try:
        if filepos_current is not None:
            stream.seek(filepos_current)
        if isinstance(filepath_or_buffer, basestring):
            # Get rid of temp stream:
            stream.close()
    except:
        pass
    try:
        return pd.read_csv(filepath_or_buffer, sep=sep, dialect=dialect, 
                encoding=encoding, doublequote=doublequote,  escapechar=escapechar, 
                lineterminator=lineterminator, quotechar=quotechar, quoting=quoting,
                skipinitialspace=skipinitialspace, header=header, names=names, skiprows=skiprows, skipfooter=skipfooter,
                warn_bad_lines=warn_bad_lines, error_bad_lines=error_bad_lines, nrows=nrows, dtype=dtype, **kwargs)
    except (IOError, OSError) as e:
        print e, filepath_or_buffer
        return pd.DataFrame()

def _encoding_heuristic(stream, confidence_threshold, encoding_preferences=('1252', '8859-1'), 
                        encoding_excludes=('koi', 'mik', 'iscii', 'tscii', 'viscii','jis','gb', 'big5', 'hkscs','ks', 'euc', '2022'),
                        encoding_default='ascii', error_no_encoding=True):
    """Return a pair of (encoding, confidence)
    Encoding_preferences overrule other code-pages to prevent mistakes. Should be changed in other countries than NL!!!"""
    pos = stream.tell()
    detector = UniversalDetector()
    i = 0
    size = 2**10
    while not (detector.done or i > 7): 
        detector.feed(stream.read(size))
        i += 1
        if size < 2 ** 16: size <<= 1
    stream.seek(pos)
    certain = detector.done
    detector.close()
    confidence, encoding = (detector.result['confidence'], detector.result['encoding']) if certain else (0.0, None)
    if not encoding:
        encoding = encoding_default
    elif confidence < confidence_threshold + .5 * (1 - confidence_threshold) and \
       (any([base in encoding.lower() for base in ('windows', 'iso')]) and not
        any([cp in encoding for cp in encoding_preferences])):
        # Heuristic: ANSI (mbcs) is much more probable than exotic windows/iso encoding:
        encoding = encoding_default
    if any([enc in encoding.lower() for enc in encoding_excludes]):
        encoding = encoding_default
        confidence = 0.0
    if encoding == 'ascii':
        # Plain ASCII is very improbable. Windows is superset of ascii, so does not hurt.
        encoding = 'mbcs'
    if confidence < confidence_threshold and error_no_encoding:
        raise ValueError('Heuristic determination of encoding failed: {:.1%} confidence in "{}", {:.1%} required.'.format(confidence, encoding, confidence_threshold))
    return encoding, confidence

def _sep_heuristic(stream, confidence_threshold, line_count=100, seps=';,\t|'):
    """Return the inferred separator and the confidence with which it is derived."""
    # Get the first 100 lines and get the delimiter with the most occurrences and the least std dev:
    # Get first count lines:
    lines = []
    for i, line in enumerate(stream):
        if i >= line_count:
            break
        lines.append(line)
    if not lines:
        return seps[0], 0.0
    results = []
    for sep in seps:
        nums = [line.count(sep) for line in lines]
        avg = float(sum(nums)) / len(nums)
        stddev = (sum([(num - avg) ** 2 for num in nums]) / len(nums)) ** .5
        if avg >= 1 and stddev / avg < 1 - confidence_threshold:
            results.append((avg, 1 - stddev / avg, sep))
    results.sort(key=lambda t: t[0] * t[1], reverse=True)
    if results:
        return results[0][-1], results[0][1]
    else:
        return seps[0], 0.0

def _dialect_heuristic(stream, confidence_threshold, line_count=100, 
                       dialects=(DialectCSV, DialectComma, DialectTab, DialectPipeNoEscape)):
    """Return the inferred dialect and the confidence with which it is derived."""
    # Get the first 100 lines and get the delimiter with the most occurrences and the least std dev:
    # Get first count lines:
    lines = []
    for i, line in enumerate(stream):
        if i >= line_count:
            break
        lines.append(line)
    if not lines:
        return dialects[0], 0.0
    results = []
    for dialect in dialects:
        nums = [line.count(dialect.delimiter) for line in lines]
        avg = float(sum(nums)) / len(nums)
        stddev = (sum([(num - avg) ** 2 for num in nums]) / len(nums)) ** .5
        if avg >= 1 and stddev / avg < 1 - confidence_threshold:
            results.append((avg, 1 - stddev / avg, dialect))
    results.sort(key=lambda t: t[0], reverse=True)
    if results:
        return results[0][-1], results[0][1]
    else:
        return dialects[0], 0.0

if __name__ == '__main__':
    from indroid.pandas.core.cache import read_source
    df1 = read_mysql(r"X:\proj\Argeweb\GB_export_20140924_stripped-small.sql")
    df1 = read_mysql(r"P:\data\source\argeweb\gb\2014-09-24\GB_export_20140924_stripped.sql")
    import sys
    sys.exit()
    df1 = read_xml(r"P:\data\source\kpn\swol_marketing\odin_billing\2009-01-15\workshop-empty\2008-03\input\2008-03_factuurbestand.xml")
    df1 = read_source('odin_billing', 'workshop-empty/2008-03/input/2008-03_factuurbestand.xml', reader_kwargs={'elementname': 'Invoice'})['2009-01-15']
    df2 = read_csv(r"P:\data\source\kpn\swol_marketing\cendris\2011-02-18\ff_ids_levering_cendris.txt", nrows=1000)
    df3 = read_csv(r"P:\data\source\kpn\swol_marketing\cendris\2013-06-20\ff_ids_levering_cendris_juli.txt", nrows=1000)