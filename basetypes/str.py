import re
import pandas as pd
from itertools import chain
# str.py - categorizers for values 

# For all categorizers, a docstring MUST be present with the date of last change in it. For automatic re-generation of the 
# values, the current value of the function (the code and the docstring) are compared to the previous used version.
# Not all code-changes propagate in func_code-co_code, so the docstring is used as extra check.

# ToDo: make all categorizers with values as index and count as values, so they can be chained without problems!!!

def _grouper(series):
    return pd.Series(series.index, series.index)

def _groupby(series, func):
    return series.groupby(_grouper(series).apply(func))

def cat99(series):
    "Identical value for counting. 2013-11-01"
    return series
def cat92(series):
    """Return a list of normalized substrings. All non-alphas are considered a separator,
    so effectively returns all separate words in a string. 2013-11-13"""
    # ToDo: get right index!!
    return series.groupby(_grouper(series).str.lower().str.strip()).sum()
    s2 = pd.Series(list(chain(*series.str.lower().str.split())))
    return s2.groupby(s2.values).size()
def cat90(series):
    "Lower cased, all non-alphanumerics (space, separators etc) are replaced by a space. 2013-11-01"
    return _groupby(series, lambda value: re.sub('\W+', ' ', value.lower()).strip()).sum()
def cat85(series):
    "Vowels, consonants, numbers, common special characters, whitespaces and other characters. 2013-11-01"
    return _groupby(series, lambda value: re.sub('[^dxsca]+', 'n', re.sub('\d', 'd', re.sub('[\.\-:;,@|]+', 'x', re.sub('\s', 's', (re.sub('[b-zB-Z]', 'c', (re.sub('[AEIOUaeiou]', 'a', value))))))))).sum()
def cat80(series):
    "Capitals,lower case, numbers, common special characters, whitespaces and other characters. 2013-11-01"
    return _groupby(series, lambda value: re.sub('[^dxscC]+', 'n', re.sub('\d', 'd', re.sub('[\.\-:;,@|]+', 'x', re.sub('\s', 's', re.sub('[A-Z]', 'C', (re.sub('[a-z]', 'c', value)))))))).sum()
def cat70(series):
    "Letters, numbers, common special characters, whitespaces and other characters. 2013-11-01"
    return _groupby(series, lambda value: re.sub('[^dxscC]+', 'n', re.sub('\d', 'd', re.sub('[\.\-:;,@|]+', 'x', re.sub('\s', 's', re.sub('[A-Za-z]', 'c', value)))))).sum()
def cat65(series):
    "Letters and numbers; all special characters and spaces map to x. 2014-03-04"
    return _groupby(series, lambda v: re.sub('[^dc]+', 'x', re.sub('\d', 'd', re.sub('[A-Za-z]', 'c', v)))).sum()
def cat60(series):
    "Letters and numbers; all special characters and spaces removed. 2013-11-01"
    return _groupby(series, lambda value: re.sub('[^dc]+', '', re.sub('\d', 'd', re.sub('[A-Za-z]', 'c', value)))).sum()
def cat40(series):
    "Only special characters; letters and digits are equal (like in random codes) 2013-11-01"
    return _groupby(series, lambda value: re.sub('[^w]','x', re.sub('[0-9a-zA-Z]', 'w', value))).sum()

def _value_compressed(value):
    "Occurrences of two or more equal characters are replaced by single character."
    if not value:
        return value
    result = [value[0]]
    for c in value[1:]:
        if result[-1] != c: result.append(c)
    return ''.join(result)
def _series_compressed(series, cat):
    return _groupby(cat(series), lambda v: _value_compressed(v)).sum()

def cat55(series):
    "Compressed: vowels, consonants, numbers, common special characters, whitespaces and other characters. 2013-11-01"
    return _series_compressed(series, cat85)
def cat50(series):
    "Compressed: capitals,lower case, numbers, common special characters, whitespaces and other characters. 2013-11-01"
    return _series_compressed(series, cat80)
def cat35(series):
    "Compressed: only special characters; letters and digits are equal (like in random codes) 2013-11-01"
    return _series_compressed(series, cat40)
def cat30(series):
    "Compressed: letters, numbers, common special characters, whitespaces and other characters. 2013-11-01"
    return _series_compressed(series, cat70)

def _value_present(value):
    "Only the presence of characters is registered and returned in a fixed order."
    return ''.join(sorted(set(value)))
def _series_present(series, cat):
    return _groupby(cat(series), lambda v: _value_present(v)).sum()

def cat26(series):
    "present: only special characters; letters and digits are equal (like in random codes) 2014-03-04"
    return _series_present(series, cat85)
def cat23(series):
    "Present: upper/lower letters, numbers, common special characters, whitespaces and other characters. 2014-03-04"
    return _series_present(series, cat80)
def cat20(series):
    "Present: letters and numbers; special characters and spaces. 2014-03-04"
    return _series_present(series, cat70)
def cat10(series):
    "Return types of characters in value, in fixed order. 2014-03-04"
    return _series_present(series, cat65)
def cat05(series):
    "Return length of value. 2014-03-04"
    return series.groupby(_grouper(series).str.len().astype(str)).sum()

def _type_init(self):
    """Contains the type-specific cleansing, validation, transformation etc.
    ToDo: completely integrate in current type-system!!!"""
    if hasattr(self, 'normalize'):
        # Make the normalize-function robust when not already:
        #self.normalize = robust_return(robust_arg_string(self.normalize))
        self.normalize = robust_return(self.normalize)
    if hasattr(self, 'pattern'):
        if isinstance(self.pattern, basestring) and not self.pattern.endswith('$'):
            #print 'Warning: {}.pattern = "{}" does not end with "$", added for validation.'.format(modulename, self.pattern)
            self.pattern += '$'
        if isinstance(self.pattern, collections.Iterable) and not isinstance(self.pattern, basestring):
            self.pattern = [pat + ('' if pat.endswith('$') else '$') for pat in self.pattern]
    flags = getattr(self, 'pattern_flags') if hasattr(self, 'pattern_flags') else 0
    validate_constructed = False
    if hasattr(self, 'pattern') and not hasattr(self, 'validate'):
        # Construct the validator. Validator receives normalized values, 
        # so normalization is left outside the function
        if isinstance(self.pattern, basestring):
            c = re.compile(self.pattern, flags)
            self.validate = lambda v: bool(c.match(str(v)))
        else:
            cs = [re.compile(pattern, flags) for pattern in self.pattern]
            self.validate = lambda v: any([bool(c(str(v))) for c in cs])
        self.validate.func_doc = '{} must match "{}"'.format(modulename, self.pattern)
        validate_constructed = True
    if hasattr(self, 'pattern'):
        # Construct two categorizers: an early categorizer to ensure inclusion of this type and
        # a late categorizer to ensure exclusion of others. The typename is returned when a match is found, 
        # the value 'no_x' with the module name inserted at the x when no match found.
        for cat_name in CAT_AUTO:
            # Do all computation and compilation outside the lambda; speeds it up many factors...
            name = modulename.split('.')[-1]
            no_name = 'no_' + name
            if isinstance(self.pattern, basestring):
                c = re.compile(self.pattern, flags)
                f = lambda s: name if c.match(s) else no_name
            else:
                cs = [re.compile(pattern, flags) for pattern in self.pattern]
                f = lambda s: name if any([c.match(s) for c in cs]) else no_name
            f.func_doc = 'value is a {} if it matches "{}"'.format(modulename, self.pattern)
            f.func_name = free_cat(cat_name)
            self.categorizers[free_cat(cat_name)] = f
    if hasattr(self, 'validate') and not validate_constructed:
        # Only validate the values if the validator was present and not constructed based on the pattern.
        # Pattern-validation is done 
        #self.validate = robust_arg_string(self.validate)
        for cat_name in CAT_VALIDATE:
            self.categorizers[free_cat(cat_name)] = self.validate
    if not hasattr(self, 'convert'):
        # convert function is missing from the type, validation and/or normalization are present.
        # construct this function based on normalization/validation
        if hasattr(self, 'normalize'):
            if hasattr(self, 'validate'):
                def vn(value):
                    value_clean = self.normalize(value)
                    return value_clean if self.validate(value_clean) else np.nan
                self.convert = vn
                self.convert.func_doc = "Validate normalized value.\n{}\n{}".format(self.validate.func_doc, self.normalize.func_doc)
            else:
                self.convert = lambda v: self.normalize(v)
                self.convert.func_doc = "Normalize value.\n{}".format(self.normalize.func_doc)
        elif hasattr(self, 'validate'):
            self.convert = lambda v: v if self.validate(v) else np.nan
            self.convert.func_doc = "Validate value.\n{}".format(self.validate.func_doc)
    if hasattr(self, 'type'):
        if hasattr(self, 'convert'):
            self.convert = robust_return_type(self.convert, self.type)
        else:
            self.convert = robust_return_type(self.type, self.type)
    if hasattr(self, 'convert'):
        # Conversion function present, constructed or original.
        # Insert validation function if necessary:
        if hasattr(self, 'validate'):
            self.convert = validate_arg(self.convert, self.validate)
    # Make all functions return a string, for all values are stored as a string in the corresponding CSV-files.
    # The validate-function returns a boolean (and it should!), but it is converted to a string as categorizer.
    #self.categorizers = {cat: robust_return_type(robust_arg_string(f)) for cat, f in self.categorizers.items()}
    self.categorizers = {cat: robust_return_type(f) for cat, f in self.categorizers.items()}    

def test_cat():
    s1 =pd.Series(pd.read_table(r"P:\data\types\nl\lastname\cendris\corpus.txt").iloc[:,0])
    for cat in (cat05, cat10, cat20, cat23, cat26, cat30, cat35, cat40, cat50, cat55, cat60, cat65, cat70, cat80, cat85, cat90, cat92, cat99):
        print cat
        print cat(s1).groupby(level=0).sum()

if __name__ == '__main__':
    test_cat()
