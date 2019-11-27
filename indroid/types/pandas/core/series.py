from __future__ import division
import os, os.path
from indroid import pandas as pd
from indroid.pandas.core.utils import mkdir

###############################################################################
# This module contains helper-functions which all work on a pandas Series.
# All functions have a series as their first argument and in may cases do nothing more than 
# enhance existing functions to make dealing with series even easier.
###############################################################################

class Series(pd.Series):
    def to_csv(self, path, index=False, sep=";", na_rep='', float_format=None, 
               header=True, index_label=None, mode='w', 
               nanRep=None, encoding='utf-8', date_format=None):
        mkdir(path)
        super(Series, self).to_csv(path, index, sep, na_rep, float_format, 
            header, index_label, mode, nanRep, encoding, date_format)

def test():
    import indroid.pandas as bp

if __name__ == '__main__':
    test()
    