import re
import numpy as np

pattern = '\d+\.\d+\.\d+\.\d+$'

def validate(value):
    "Pattern must match and all values must be <= 255. 2013-11-01"
    if not re.match(pattern, value):
        return False
    return all([0 <= int(s) <= 255 for s in value.split('.')])

def normalize(value):
    "Remove all non-digits and non-dots; remove trailing zeroes from numbers."
    v = re.sub('[^0-9\.]', '', value)
    return '.'.join([n.lstrip('0') or '0' for n in v.split('.')])

def convert(value):
    try:
        t = [int(i) for i in value.split('.')]
        return (t[0] << 24) + (t[1] << 16) + (t[2] << 8) + t[3]
    except (ValueError, TypeError, IndexError, AttributeError):
        return np.nan

def to_string(value):
    "Return a dotted representation from the internal format (a long/int)."
    i = long(value)
    return '{}.{}.{}.{}'.format((i>>24) % 256, (i>>16) % 256, (i >> 8) % 256, i % 256)

if __name__ == '__main__':
    for value in 'test-ip; 12.12.12.12; 10.0.0,2; 1.1.1.1  ; 10.0.0.345 ; test@testnl'.split(';'):
        print value, normalize(value), validate(normalize(value))
        print dataframe_to_human(source_to_dataframe(value))