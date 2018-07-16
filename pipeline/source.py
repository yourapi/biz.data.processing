# Module for functions on a dataframe. 
# Every function must have one argument: the dataframe.
# Every function should return a dataframe with the result of the operation.
# Every function should have a doc-string with date last changed, so this can be used 
# for determining if a function has changed, for automatic cache-management.
# Automatic cache-mgmt can be overridden by supplying a function with the same name, suffixed by _changed
# Automatic hash-generation can be overruled by supplying a function with the same name, suffixed by _hash
# This should receive a dataframe and return whether the function should be performed.
import pandas as pd
import isounidecode as _isounidecode, hashlib as _hashlib

def input(df):
    """Read the specified file with the specified arguents (if present). The string contents are flattened:
    all special characters (e.g. with diacrits, chinese etc) are converted to ascii-characters."""
    import biz.pandas as bp
    result = bp.read_file(df._biz_processor.filename, **df._biz_processor.reader_kwargs).applymap(_flatten)
    return result if result is not None else pd.DataFrame()

def map(df):
    """Return a dataframe with all columns mapped to the best fitting type. When only 1 (should be parameter in future!)
    column of a certain type is found, rename it to the name of the type. When e.g. one column named 'POSTCODE_CORR' 
    contains a zipcode, it is renamed to 'zipcode'. When more than one is found, keep the original names.
    Last changed: 2014-02-16"""
    return df._biz_metadata.map(df)

def map_hash(df):
    """Return the attrs from the metadata-file, if any.
    Also include the hash for all conversion/validation-functions used by the specified types.
    So when one of these functions changes, the complete hash changes and the contents are computed again. (ToDo!)"""
    from biz.pandas.core.cache import mystat
    from os.path import exists
    filename = df._biz_metadata.filename
    h = _hashlib.md5(str(mystat(filename) if isinstance(filename, basestring) and exists(filename) else filename))
    h.update(df._biz_metadata.function_hash('convert'))
    return h.hexdigest()

def empty_value(df):
    return df.applymap(lambda v: '' if isinstance(v, basestring) and v == '$' else v)

def _flatten(s):
    """pytables only stores plain ascii. For precision data (like mailings), the original data
    must be retrieved (tbd)"""
    if not isinstance(s, basestring):
        return s
    try:
        return s.encode('ascii')
    except:
        try:
            return _isounidecode.unidecode(s)
        except:
            return ''

