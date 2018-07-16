from __future__ import division
import os, os.path

###############################################################################
# This module contains helper-functions which all work on a pandas Series.
# All functions have a series as their first argument and in may cases do nothing more than 
# enhance existing functions to make dealing with series even easier.
###############################################################################

def mkdir(path, contains_file=True):
    "Creates the path if it does not exist. If path is no string, ignore it and take no action."
    if isinstance(path, basestring):
        if contains_file:
            path = os.path.split(path)[0]
        if not os.path.exists(path):
            try:
                os.makedirs(path)
            except OSError:
                pass

def test():
    import biz.pandas as bp

if __name__ == '__main__':
    test()
    