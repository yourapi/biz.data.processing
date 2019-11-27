import numpy as np
from indroid import pandas as pd
from dateutil.parser import parse

def match(series, threshold=1.0):
    """The supplied series (a column values of a dataframe) is inspected for match for 
    the specified type (datetime).
    The values must have at least two separate numbers and be parsable by dateutil.parser.
    Returns two values: <True> or <False> for the match and a dictionary with optional keywords for the parser."""
    pass

def convert(value, **kwargs):
    """Ensure the converted value is a timestamp; because the to_datetime function can also return 
    other types if supplied with types like bool, None. Return timestamp or nan to ensure the function
    is not wrapped in a validation-function. 2013-03-05"""
    try:
        v = pd.to_datetime(value)
        return v if isinstance(v, pd.Timestamp) else np.nan
    except ValueError:
        return np.nan

if __name__ == '__main__':
    pd.read_csv
