import pandas as pd
import re, os
from os.path import join, split, splitext
from functools import partial
from biz.pandas.io.parsers import read_csv, read_excel, read_xml
from pandas import read_html

###############################################################################
# This module contains helpder-functions which all work on a pandas DataFrame.
# All functions have a dataframe as their first argument and in may cases do nothing more than 
# enhance existing functions to make deialing with datafranmes even easier.
###############################################################################

def read_file(filepath_or_buffer, reader=None, reader_by_extension=((('csv', 'txt'), partial(read_csv, error_bad_lines=False)),
                                                                    (('xls', 'xlsx', 'xlsm'), read_excel),
                                                                    (('xml',), read_xml),
                                                                    (('htm.*',), read_html)), **kwargs):
    """Choose the appropriate reader based on the extension of the file, if a filename is given.
    ToDo: support for XML-reading."""
    name = filepath_or_buffer if isinstance(filepath_or_buffer, basestring) else filepath_or_buffer.name
    ext_file = splitext(name)[1][1:]
    if not reader:
        for exts, rdr in reader_by_extension:
            for ext in exts:
                if re.match(ext, ext_file, re.I):
                    reader = rdr
                    break
            if reader: break
    if reader: return reader(filepath_or_buffer, **kwargs)

if __name__ == '__main__':
    df0 = read_file(r"P:\metadata\source\kpn\swol_marketing\odin\metadata3.csv")
    df1 = read_file(r"P:\data\source\kpn\swol_marketing\cendris\Van_Cendris_20130523\ff_ids_levering_cendris_mei.txt", nrows=1000)
    df1a = read_file(r"P:\data\source\kpn\swol_marketing\cendris\Van_cendris_20120120\ff_ids_levering_cendris_jan.txt", nrows=1000)
    df1x = read_file(r"P:\data\source\kpn\swol_marketing\iaso_benchmark\2013-08-20\KPN-repaired-2013.08.20 benchmark.xml")
    df2 = read_file(r"P:\data\source\common\KvK\2009-09-01\output\SBI-codes 2009.xls")
    df3 = read_file(r"P:\data\source\common\KvK\2010-08-19\SBI 2008 codes_tcm73-193439.xls")
    df4 = read_file(r"P:\data\source\kpn\swol_marketing\iaso\2013-01-28\BOL overzicht storage usage 2013-01-28.xlsx")
    df5 = read_file(r"P:\data\source\kpn\swol_marketing\f-secure\2010-08-09\keys in gebruik.xlsx", sheetname='Sheet1')