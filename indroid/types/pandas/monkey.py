###############################################################################
# This module contains the functions to make pandas object biz-aware.
# The method is identical to the gevent monkey-patching, so is invoked by calling patch_all
# Monkey-patching is restricted to the absolute minimum; the derived classes are 
# invoked instead of the original classes. 
# At this moment, only one case is known where this doesn't work: in to_hdf a 
# check on class name is done for which it is necessary to adapt the class with the 
# standard class.
###############################################################################
from indroid.pandas import Series, DataFrame
from indroid import pandas as pd


def patch_series():
    def __new__(cls, *args, **kwargs):
        return Series()  # No instantiation; this is done anyway by the dataframe (not standard!)
        # return DataFrame(*args, **kwargs)  # This would be standard; this results in double initialization 
        # due to internal workings pandas
    pd.Series.__new__ = staticmethod(__new__)    
    Series.__new__ = object.__new__  # Instantiate parent-object of parent-object, because parent-object instantiates this class. object is first class with __new__ method; other ancestors don't define it.

def patch_dataframe():
    def __new__(cls, *args, **kwargs):
        return DataFrame()  # No instantiation; this is done anyway by the dataframe (not standard!)
        # return DataFrame(*args, **kwargs)  # This would be standard; this results in double initialization 
        # due to internal workings pandas
    pd.DataFrame.__new__ = staticmethod(__new__)    
    DataFrame.__new__ = object.__new__  # Instantiate parent-object of parent-object, because parent-object instantiates this class. object is first class with __new__ method; other ancestors don't define it.

def patch_all(dataframe=True, series=True):
    if dataframe: patch_dataframe()
    if series: patch_series()

def test():
    patch_all()

if __name__ == '__main__':
    test()
    