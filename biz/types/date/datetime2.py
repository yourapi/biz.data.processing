#import importlib
#strptime = importlib.import_module('datetime').datetime.strptime  # Do a complicated import because the datetime-module is masked by the current module with the same name.
import numpy as np
from biz import pandas as pd

pattern = '\d+/\d+/\d+ \d+:\d+:\d+'

def convert(value):
    """Ensure the converted value is a timestamp; because the to_datetime function can also return 
    other types if supplied with types like bool, None. Return timestamp or nan to ensure the function
    is not wrapped in a validation-function. 2013-03-05"""
    try:
        v = pd.to_datetime(value)
        return v if isinstance(v, pd.tslib.Timestamp) else np.nan
    except ValueError:
        return np.nan
