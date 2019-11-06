from __future__ import division
from biz import pandas as pd

###############################################################################
# This module contains helper-functions which all work on a pandas DataFrame.
# All functions have a dataframe as their first argument and in may cases do nothing more than 
# enhance existing functions to make dealing with datafranmes even easier.
# The functions replace the original pandas-functions, renaming the original functions
# with _org_(name)
###############################################################################

pd._org_concat = pd.concat
def concat(objs, axis=0, join='outer', join_axes=None, ignore_index=False,
           keys=None, levels=None, names=None, verify_integrity=False):
    result = pd._org_concat(objs, axis=axis, join_axes=join_axes,
                       ignore_index=ignore_index, join=join,
                       keys=keys, levels=levels, names=names,
                       verify_integrity=verify_integrity)
    if isinstance(result, pd.DataFrame):
        from biz.pandas.tools import _Concatenator
        c = _Concatenator(objs, axis=axis, join_axes=join_axes,
                          ignore_index=ignore_index, join=join,
                          keys=keys, levels=levels, names=names,
                          verify_integrity=verify_integrity)
        # ToDo: meaningful adding of metadata and processes!
        try:
            result._biz_metadata.add(*[o._biz_metadata for o in c.objs if isinstance(o, pd.DataFrame)])
            result._biz_processor.add(*[o._biz_processor for o in c.objs if isinstance(o, pd.DataFrame)])
        except AttributeError:
            # Dataframe was not subclassed. Can happen in 'strange' environments like ipython parallel.
            pass
    return result

concat.__doc__ = "Special biz-concat, which adds metadata to result, if applicable.\n" + pd.concat.__doc__
pd.concat = concat

def test_clean():
    import biz.pandas as bp
    odin = bp.read_source('odin', 'extract/odin_dump\.csv', max_level_date=0, reader_kwargs={'nrows': 10000})[-1]
    o1 = clean_column_names(odin, inplace=False)

if __name__ == '__main__':
    test_clean()
    