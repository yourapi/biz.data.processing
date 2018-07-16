from __future__ import division
import numpy as np, pandas as pd
from functools import partial
import math

def _dist_log(series, base):
    """Return the number of occurrences for each value, divided in log(base)-chunks.
    This is an indication of the spread of the values. 2014-03-25"""
    dist = _log_cat(base)(series.value_counts()).astype(str)
    return pd.Series([1] * len(dist), dist.values)

def _log_cat(base):
    def get_log(series, base):
        log_base = math.log(base)
        def cat(v):
            try:
                return int(math.log(v) / log_base)
            except ValueError:
                return ''
        return series.fillna(0).abs().apply(cat).astype(str)
    return partial(get_log, base=base)

def _log_log(series, base):
    return pd.Series(series.replace(0, 1).apply(lambda v:1/v).values, _log_cat(base)(series).values)

def _log_lin(series, base):
    return pd.Series([1] * len(series), _log_cat(base)(series).values)

def cat99(series):
    d = series.value_counts()
    return pd.Series(d.values, d.index.astype(str))

def cat95(series):
    dist = series.value_counts().astype(str)
    return pd.Series([1] * len(dist), dist.values)

def cat94(series):
    """Return the number of occurrences for each value, divided in log(1.15)-chunks.
    This is an indication of the spread of the values. 2014-03-25"""
    return _dist_log(series, 1.15)

def cat93(series):
    """Return the number of occurrences for each value, divided in log(1.5)-chunks.
    This is an indication of the spread of the values. 2014-03-25"""
    return _dist_log(series, 1.5)

def cat92(series):
    """Return the number of occurrences for each value, divided in log(4)-chunks.
    This is an indication of the spread of the values. 2014-03-25"""
    return _dist_log(series, 4)

def cat91(series):
    """Return the number of occurrences for each value, divided in log(10)-chunks.
    This is an indication of the spread of the values. 2014-03-25"""
    return _dist_log(series, 10)

def cat56(series):
    "Return the count to enable logarithmic distribution. 2014-03-24"
    return _log_lin(series, 1.15)

def cat54(series):
    "Return the count to enable logarithmic distribution. 2014-03-24"
    return _log_lin(series, 1.5)

def cat52(series):
    "Return the count to enable logarithmic distribution. 2014-03-24"
    return _log_lin(series, 4)

def cat50(series):
    "Return the count from log(10)-categories to enable logarithmic distribution. 2014-03-24"
    return _log_lin(series, 10)

def cat24(series):
    "Return the inverse value divided in log(1.15)-categories to enable logarithmic distribution. 2014-03-24"
    return _log_log(series, 1.15)

def cat22(series):
    "Return the inverse value divided in log(1.5)-categories to enable logarithmic distribution. 2014-03-24"
    return _log_log(series, 1.5)

def cat20(series):
    "Return the inverse value divided in log(4)-categories to enable logarithmic distribution. 2014-03-24"
    return _log_log(series, 4)

def cat05(series):
    "Return the inverse value divided in log(10)-categories to enable logarithmic distribution. 2014-03-24"
    return _log_log(series, 10)

def _is_long(v):
    try:
        long(v)
        return True
    except ValueError:
        return False

def match(series, threshold=1.0):
    """The supplied series (a column values of a dataframe) is inspected for match for 
    the specified type (int).
    Returns two values: <True> or <False> for the match and a dictionary with optional keywords for the parser."""
    try:
        # Converting with ufunc is fastest. If it succeeds, the values are long:
        s = series.astype(long)
        return True, {}
    except ValueError:
        # No clean conversion possible. No fancy conversion possible, so check if proportion is convertible to int:
        if threshold == 1.0:
            return False, {}
        # Now see if proportion is fitting for number of non-longs:
        return (len(series[series.apply(_is_long)]) / len(series) >= threshold), {}

def convert(series, **kwargs):
    """Ensure the converted value is a long.  2013-03-05"""
    try:
        # Converting with ufunc is fastest. If it succeeds, we're done:
        return series.astype(long)
    except ValueError:
        good = series.apply(_is_long)
        series.ix[good] = series.ix[good].astype(long)
        series.ix[~good] = np.nan
        return series

def _test():
    s =pd.Series(range(10)).astype(str)
    print match(s)
    s = '\t'+ s + ' '
    print match(s)
    print convert(s), convert(s) ** 2
    s[5] = 'five'
    print match(s)
    print match(s, .9)
    print convert(s), convert(s)**4
    cats = (cat05, cat20, cat22, cat24, cat50, cat52, cat54, cat56, cat99)
    for cat in cats:
        print cat
        print cat(convert(s)**4).groupby(level=0).sum()
    s = s.astype(str) + '.0'
    print match(s)
    print convert(s)
    s1 = pd.Series(range(1000))
    s2 = pd.Series(list(range(10)) + list(range(5, 100)) + list(range(50, 150)) + list(range(60, 80)) * 4 + [333]*100 + [444]*498 +[555]*12554)
    from itertools import chain
    s3 = pd.Series(list(chain(*[list(range(i)) for i in range(100)])))
    print cat99(s3)
    for series in (s1, s2, s3):
        for cat in cats:
            print cat
            print cat(series).groupby(level=0).sum()

if __name__ == '__main__':
    _test()
