# types.py - Find and compile all available converters for all available types.
# The types are implied from the data-directory and are tried to import. When import fails, 
# apparently no converter exists for the specified level.
from __future__ import division
import sys, os, re, importlib, hashlib, itertools, math, collections
from os.path import join, split, splitext, exists
import numpy as np
from indroid import pandas as pd
from indroid.basetypes.str import *  # Temporary solution for string-only categorizers

CAT_PAT = 'cat(\d+)'
DEFAULT_CAT = 'cat99'
CAT_AUTO = ('cat15', 'cat75')  # The automatic categorizers to include values in the comparison
CAT_VALIDATE = ('cat16', 'cat76')  # The places for the validators. Are not included when automatically derived from pattern

def index_dropna(df, inplace=True):
    "Takes a dataframe and remove dropna from the index."
    overlap = df.index.drop(df.index & pd.Index([np.nan, '']))
    if inplace:
        df = df[overlap]
    else:
        return df[overlap]

def chi_square(series1, series2):
    "Return the relative chi-square as a proportion of the maximum difference. Hence 0 means identical, 1 means totally different."
    try:
        index_dropna(series1)
        index_dropna(series2)
        series3 = series1 * (series2.sum() / series1.sum()) # Scale the series to equal numbers
        series2 = series2.groupby(level=0).sum()
        series3 = series3.groupby(level=0).sum()
        delta = index_dropna(series3.sub(series2, fill_value=0) ** 2, inplace=False)   # Sum of squares of differences.
        maxi = index_dropna(series3.combine(series2, max, 0) ** 2, inplace=False)      # Sumof squares of maximum differences
        return delta.sum() / maxi.sum()
    except ZeroDivisionError:
        # No valid 
        return 1

class Corpus(object):
    """Contains the data for a corpus, including all categorized versions. The data of a specific category can be 
    retrieved by specifying the category as an index: corpus['cat10']
    The values of the category are cached in """
    def __init__(self, path, typ, categorizers={}, file_corpus='corpus.txt', file_cat='corpus.{}.csv', significance=.9):
        self.path = path
        self.name = split(path)[-1]
        self.typ = typ
        self.categorizers = categorizers
        self.file_corpus = file_corpus
        self.file_cat = file_cat
        self.significance = significance
        self.frequencies = {}
        self._significances = None
        self._function_checksums = None
        self.data = None
    def function_names(self, func):
        h = hashlib.md5(func.func_code.co_code)
        if func.func_doc:
            h.update(func.func_doc)
        name = func.func_name
        filename = join(self.path, self.file_cat.format('functions'))
        #print name, h.hexdigest(), dotted(split(filename)[0], r"P:\data\types")
        return name, h.hexdigest(), filename
    def significance_get(self):
        filename = join(self.path, self.file_cat.format('significance'))
        return filename, \
               pd.read_csv(filename, sep=';', header=0, index_col=(0,1), dtype={'count': long}) \
               if exists(filename) else pd.DataFrame()
    def significance_count(self, cat, significance=None):
        "Return the minimum number of categories to reach a specified level of significance."
        if cat == DEFAULT_CAT: return None
        significance = significance or self.significance
        if self._significances is None:
            _, self._significances = self.significance_get()
        # The significance has a two-level index:first level is category, second level is the level [0,1)
        try:
            s1 = self._significances.loc[cat]
            return s1[s1.index >= significance].min()[0] + 3  # Add an arbitrary small number to prevent 
            #  brittle comparisons in small categories. Small number makes hardly no digfference in performance
        except:
            return None
    def significance_save(self, cat, series, levels=(.5, .75, .9, .95, .99, .999)):
        """Save the minimum number of categories needed for the specified levels of 'coverage'
        The coverage specifies the maximum change in chi-square if the categories up to the specified 
        category are included. This way a (generally small) set of categories is specified."""
        filename, df = self.significance_get()
        maxi = (series[series > 2].astype(long) ** 2)
        q = (maxi / maxi.sum()).cumsum()
        if len(q) == 0: q = pd.Series([1])
        # Make a dataframe of the current results and add the existing results:
        result = {(cat, level): len(q[q<=level]) + 1 for level in levels}
        df1 = pd.DataFrame(result.values(), pd.MultiIndex.from_tuples(result.keys(), names=('category', 'significance')), 
                           columns=['count']).combine_first(df)
        # Bug in pandas: combined value of longs becomes a float
        df1['count'] = df1['count'].astype(long)
        df1.to_csv(filename, sep=';', index=True)
        # Invalidate the cache:
        self._significances = None
    def function_changed(self, func):
        "Compare the code of the supplied function with the stored function. Return False if same, True if different."
        name, checksum, filename = self.function_names(func)
        try:
            if self._function_checksums is None:
                self._function_checksums = pd.read_csv(filename, sep=';', header=0, index_col='function')
                print ',',
            return self._function_checksums.ix[name]['checksum'] != checksum
        except:
            # Any exception: file does not exist, wrong file etc: no match
            return True
    def function_save(self, func, force=False):
        "Save the function and the checksum of the function in the specified location."
        if not (self.function_changed(func) or force):
            # No change and no force save, exit:
            return
        name, checksum, filename = self.function_names(func)
        df = pd.read_csv(filename, sep=';', header=0, index_col='function') if exists(filename) else pd.DataFrame()
        pd.DataFrame([checksum], pd.Index([name], name='function'), ['checksum']).combine_first(df).to_csv(filename, sep=';', index=True)
        # Invalidate the cache:
        self._function_checksums = None
    def __getitem__(self, name):
        if name not in self.frequencies:
            # Frequencies not filled, try to get the filename:
            filename = join(self.path, self.file_cat.format(name))
            filename_corpus = join(self.path, self.file_corpus)
            filename_default_cat = join(self.path, self.file_cat.format(DEFAULT_CAT))
            # Has the code changed? If so, remove the cache and store the new code:
            func = self.categorizers[name]
            # See if the corpus is refreshed. If so, delete the cat-file so it is regenerated:
            up_to_date = False
            if exists(filename):
                up_to_date = True
                if exists(filename_corpus):
                    if (os.path.getmtime(filename_corpus) > os.path.getmtime(filename)) or self.function_changed(func):
                        # Modification time of corpus is later, so remove the cache:
                        up_to_date = False
                # Special case. See if normalization-function has changed; this changes all downstream data:
                if hasattr(self.typ, 'normalize') and self.function_changed(self.typ.normalize):
                    up_to_date = False
                if exists(filename_default_cat):
                    if (os.path.getmtime(filename_default_cat) > os.path.getmtime(filename)):
                        # Modification time of default cat (with counts per value) is later, so remove the cache:
                        up_to_date = False
            if up_to_date:
                # Data already in file. Get it:
                try:
                    df = pd.read_csv(filename, sep=';', header=0, nrows=self.significance_count(name), dtype={'value': str, 'count': long})
                    self.frequencies[name] = pd.Series(df['count'].values, df['value'])
                except:
                    # Something went wrong. Read data from source and write again:
                    up_to_date = False
            if not up_to_date:
                # Generate the data; store it in the cache and write it to a file for future use:
                if self.data is None:
                    # Pff, raw data not present. Is categorized data present?
                    if name == DEFAULT_CAT:
                        # Get the source and count the occurrences:
                        lines = pd.Series(pd.read_table(open(join(self.path, self.file_corpus)), header=None, dtype=str).iloc[:,0])
                        # If a normalize-function is present,apply it:
                        #ToDo: apply normalize function; see if it has changed; if it has changed, invalidate DEFAULT_CAT
                        grp = lines.groupby(lines).size()
                        if hasattr(self.typ,'normalize'):
                            # Normalization required. First do group-by to save on multiple 
                            # occurrences, then normalize, then group again:
                            grp = grp.groupby(pd.Series(grp.index, grp.index).apply(self.typ.normalize)).sum()
                            self.function_save(self.typ.normalize)
                        self.data = grp
                    else:
                        self.data = self[DEFAULT_CAT]
                    # Remove NaN from index:
                    index_dropna(self.data)
                # The data is present; categorize it and write to file:
                s2 = index_dropna(func(self.data), inplace=False)
                #s3 = s2.groupby(level=0).sum()
                #s3.sort(ascending=False)
                #s3.to_csv(filename, sep=';', header=['count'], index_label=['value'], index=True)
                s2.sort(ascending=False)
                s2.to_csv(filename, sep=';', header=['count'], index_label=['value'], index=True)
                self.function_save(func)
                self.significance_save(name, s2)
                self.frequencies[name] = s2 if name == DEFAULT_CAT else s2.iloc[:min(len(s2), self.significance_count(name))]
        return self.frequencies[name]

# Define the exceptions which are caught when calling normalization/categorization-functions.
# The exceptions must only be type-related, 'real' errors (functional, otherwise) MUST still be raised!
# Otherwise, a functional error in a normalization-function (like forgetting to import the re-module!)
# will not be caught but handled silently!
CAUGHT_EXCEPTIONS = (TypeError, AttributeError, ValueError)
PARAMETER_TRIES = ('1', 1, None, True, 1.0, '', '0' * 120, np.NaN)

def robust_return_type(f, typ=str):
    """Returns the function itself if it handles all input and returns the specified type.
    If any of the inputs result in an exception, add an error handler which converts it to
    the specified type."""
    def wrapped(*args, **kwargs):
        try:
            return typ(f(*args, **kwargs))
        except CAUGHT_EXCEPTIONS:
            return np.nan
    try:
        wrapped.func_name = f.func_name
        wrapped.func_doc = f.func_doc
    except CAUGHT_EXCEPTIONS:
        pass
    try:
        if all([(isinstance(f(value), typ) or (f(value) is np.nan)) for value in PARAMETER_TRIES]):
            return f
        else:
            return wrapped
    except CAUGHT_EXCEPTIONS:
        return wrapped

# Robust arg string is not used anymore. It is the resposibility of the framework to ensure proper types for the input.
# If e.g. it is known that a function only accepts a float, the values are converted by the caller.
def robust_arg_string(f):
    """Try a number of types for first argument. If any error, wrap it in error-handler which converts
    all arguments to a string when a specified exception of any in the list CAUGHT_EXCEPTIONS is raised."""
    def wrapped(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except CAUGHT_EXCEPTIONS:
            args = [unicode(v) for v in args]
            kwargs = {k:unicode(v) for k in kwargs}
            return f(*args, **kwargs)
    return robustify(f, wrapped)

def robust_return(f):
    """Return the function itself if it handles all values well. If not, add an error handler which
    returns the argument if an error occurs, without changing it."""
    def wrapped(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except CAUGHT_EXCEPTIONS:
            return args[0] if len(args) == 1 else args
    return robustify(f, wrapped)

def robustify(f, wrapped):
    try:
        for value in PARAMETER_TRIES:
            f(value)
        # All went well, no extra exception handling necessary:
        return f
    except CAUGHT_EXCEPTIONS:
        try:
            wrapped.func_name = f.func_name
            wrapped.func_doc = f.func_doc
        except CAUGHT_EXCEPTIONS:
            pass
        return wrapped
    except Exception as e:
        # Function doesn't handle well; wrap it in handler after warning:
        print e
        raise Exception('Unexpected exception in function <{}> in type-module <{}>: {}, {}'.format(f.func_name, f.__module__, type(e).__name__, e), e)

def validate_arg(f, validate):
    def wrapped(*args, **kwargs):
        try:
            return f(*args, **kwargs) if validate(*args, **kwargs) else np.nan
        except CAUGHT_EXCEPTIONS:
            return np.nan
    return robustify(f, wrapped)

class Type(object):
    "A datatype with pointers to their possible converters, full data source and sample data sources."
    def __init__(self, name, root_module):
        def is_cat(name, value):
            return re.match(CAT_PAT, name, re.I) and callable(value)
        def free_cat(name):
            while name in self.categorizers:
                r = re.match(CAT_PAT, name)
                name = name[r.regs[0][0]:r.regs[1][0]] + str(int(r.group(1)) + 1)
            return name
        self.name = name
        self.corpus = {}
        # ToDo: get categorizers for specified basetype!!!
        self.categorizers = {k:v for k,v in sys.modules[__name__].__dict__.items() if is_cat(k, v)}
        # import the specified module; give it a unique name:
        modulename = root_module.rstrip('.') + '.' + name.strip('.')
        try:
            importlib.import_module(modulename)
            # Get the module:
            module = sys.modules[modulename]
            for k, v in module.__dict__.items():
                if not k.startswith('__') and not type(v) is type(module):
                    # Non-module-type object; function, constant etc:
                    if is_cat(k, v):
                        self.categorizers[k] = v
                    else:
                        setattr(self, k, v)
        except ImportError:
            # No module for converters found. Object gets no extra functions.
            print 'Module {} could not be loaded; no functions?'.format(modulename)
    def function_hash(self, function_name):
        h = hashlib.md5()
        if hasattr(self, function_name) and callable(getattr(self, function_name)):
            f = getattr(self, function_name)
            h.update(f.func_code.co_code)
            if f.func_doc:
                h.update(f.func_doc)
        return h.hexdigest()
    def categorizer_hash(self, highest_cat):
        h = hashlib.md5()
        for key in sorted([k for k in self.categorizers.keys() if k <= highest_cat]):
            f = self.categorizers[key]
            h.update(f.func_code.co_code)
            if f.func_doc:
                h.update(f.func_doc)
        return h.hexdigest()
    def add_corpus(self, corpus):
        self.corpus[corpus.name] = corpus
    def match(self, series, cat_min=0, cat_max=30, sample_count=None, samples_per_bin=None, previous_match=pd.Series()):
        """Return a series with the matches of each category. When a sample count of None is given, 
        the sample count is inferred from the distribution of the target, the number of bins in the 
        target is multiplied by the number of samples per bin."""
        def sample_get(count):
            count = count or 1
            i = int(len(series.dropna()) / count) or 1
            s = series.dropna()[::i][:count]
            return s
        result = {}
        if not sample_count and not samples_per_bin: 
            sample = series.dropna()
        if sample_count:
            sample = sample_get(sample_count)
        elif samples_per_bin:
            # Get the count for 7 items per bin:
            count = max([len(c[cat]) for c in self.corpus.values()]) * samples_per_bin
            # If the reference has fewer items, lower the number in the sample:
            if c[cat].sum() < count: count = c[cat].sum()
            sample = sample_get(count)
        sample1 = sample.apply(self.normalize) if hasattr(self, 'normalize') else sample
        for cat in sorted(self.categorizers.keys()):
            if not cat_min <= int(re.match(CAT_PAT, cat).group(1)) < cat_max:
                continue
            sample2 = self.categorizers[cat](sample1.groupby(sample1.values).size())
            index_dropna(sample2)
            sample3 = sample2.groupby(level=0).sum()
            result[cat] = min([chi_square(c[cat], sample3) for c in self.corpus.values()] or [1])  # Empty sequence: chi = 1
        return previous_match.append(pd.Series(result))
    @property
    def property_name(self):
        return self.name.split('.')[-1]
    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, self.name)

def dotted(dirpath, relative_to="", root_module=""):
    """Return the dotted representation of the dirpath, with directory separators replaced with dots.
    If a relative root is given, remove it from the start of the dirpath."""
    def path_components(dirpath):
        d, p = split(dirpath)
        if d == dirpath:
            return [d]
        else:
            return path_components(d) + [p]
    dirs = path_components(dirpath)
    relatives = path_components(relative_to)
    while relatives and dirs and dirs[0] == relatives[0]:
        dirs, relatives = dirs[1:], relatives[1:]
    if root_module:
        roots = root_module.split('.')
        while roots and dirs and dirs[0] == roots[0]:
            dirs, roots = dirs[1:], roots[1:]
    return '.'.join(dirs)

TypeMatch = collections.namedtuple('TypeMatch', ['type', 'match'])

def null(*args, **kwargs): pass

class Types(object):
    """All available types with pointers to their converters, """
    def __init__(self, root_data=r"P:\data\types", root_module="biz.types"):
        self.root_data = root_data
        self.root_module = root_module
        self.types = {}
    def _load(self):
        """Load the types, based on two locations:
        - corpus- and categorization-data in the data-root
        - class-information in the root of the type and below.
        The information of both sources is combined;a type can be present in any or both of these sources."""
        def type_module(path, sys_root):
            """Return the modulename and module  belonging to the specified path if it is a valid type-module. 
            If not, return (None, None)."""
            no_mod = None, None
            if splitext(path)[-1].lower() != '.py'or split(path)[-1].startswith('_'):
                return no_mod
            mod_name = dotted(splitext(path)[0], p_sys)
            shortname = dotted(splitext(path)[0], p_sys, self.root_module)
            mod = importlib.import_module(mod_name)
            for fname in 'convert normalize validate type'.split():
                if hasattr(mod, fname) and callable(getattr(mod, fname)):
                    return shortname, mod
            if hasattr(mod, 'pattern') and isinstance(mod.pattern, basestring):
                return shortname, mod
            return no_mod
        for dirpath, dirnames, filenames in os.walk(self.root_data):
            # All subdirectories in this directory contain types. A type is present if a subdir (source name) 
            # exists with a file names corpus.*.txt.
            if any([re.match('corpus.*\.txt', filename, re.I) for filename in filenames]):
                # Directory contains corpus values. The current directory is a corpus name;
                # parent directory then must be a typename:
                name = dotted(split(dirpath)[0], self.root_data)
                typ = self.types.setdefault(name, Type(name, self.root_module))
                typ.add_corpus(Corpus(dirpath, typ, typ.categorizers))
        # Now get all modules from the type-root, which were not imported by getting the corpus-files:
        mod = importlib.import_module(self.root_module)
        for p_mod in mod.__path__:
            # Get all modules, starting at the root-module:
            for p_sys in sys.path:
                if dotted(p_mod, p_sys) == self.root_module:
                    break
            for dirpath, dirnames, filenames in os.walk(p_mod):
                for filename in filenames:
                    mod_name, mod = type_module(join(dirpath, filename), p_sys)
                    if mod_name and mod_name not in self.types:
                        self.types.setdefault(mod_name, Type(mod_name, self.root_module))
        # Data is loaded. Don't load again when called, so replace _load function with stub:
        self.__class__._load = null
    def matches(self, series):
        """Return all possible matches and their match with the target. The match is the relative chi-square, 
        so 0 is total match and 1 is total difference.
        The matches are returned as a list of TypeMatch-tuples: a type and the found match of the type."""
        self._load()
        def filtered(matches, min_threshold=None, min_match_fraction=None):
            "Accept a list of tuples: Type and chi-squares per category. Return a list of possible types and their matches."
            # ToDo: make the filtering more relative: make match possible for partial match for e.g. company-names.
            def threshold(matches):
                "Return the threshold for the chi, where it is defined as lowest value + 1 stddev."
                # Make a list of the square of the matches:
                m = pd.Series([(match[1] ** 2).sum() / len(match[1]) for match in matches])
                # Get the lowest value + 1 stddev:
                sdev = m.std()
                if str(sdev) == 'nan':
                    sdev = 0.0
                print '{:.3} {:.3}'.format(math.sqrt(m.min()), sdev)
                return  math.sqrt(m.min()) + sdev
            def threshold_prop(series, threshold):
                return len(series[series < threshold]) / len(series)
            # Find the types with the largest number of matches. A match is a total chi which is smaller than the threshold.
            # The number of categories for which a match is found, is returned.
            t = threshold(matches)
            if min_threshold and min_threshold < t:
                t = min_threshold
            t_prop = (max([threshold_prop(m[1], t) for m in matches]) if matches else 0) or .9 \
                if min_match_fraction is None \
                else min_match_fraction
            # Return all types with the same number of chis which are <threshold:
            result = [m for m in matches if threshold_prop(m[1], t) >= t_prop]
            result.sort(key=lambda match: match[1].mean())
            return result
        # Execute a funnel of selections, the first three steps on a fixed sample (sampling is a relatively expensive process)
        # Take 47 because it is a prime, with approx 7 bins with each 7 values, assuming a small number of bins and high discrimination
        sample = series.dropna()[::int(len(series.dropna()) / 47) or 1][:47]
        # First establish the base type of the values (string, int, float, datetime, bool). 
        # Only a string can be upcast to a more specific type, if a type is already specific, 
        # no further conversion is done.
        basetype = None
        matches = []
        # Set the min threshold to .7 for the first categroies, because these are so basic that they MUST ALL conform.
        matches.append(filtered([TypeMatch(typ, typ.match(sample, 0, 18)) for typ in self], .7, .95))  #; print len(matches[-1]), matches[-1]
        if len(matches[-1]) > 1:
            matches.append(filtered([TypeMatch(tm.type, tm.type.match(sample, 18, 30, previous_match=tm.match)) for tm in matches[-1]]))  #; print len(matches[-1]), matches[-1]
        if len(matches[-1]) > 1:
            matches.append(filtered([TypeMatch(tm.type, tm.type.match(sample, 30, 60, previous_match=tm.match)) for tm in matches[-1]]))  #; print len(matches[-1]), matches[-1]
        if len(matches[-1]) > 1:
            # Take a new, larger sample because the categorizers generally have a larger populatoin in these area,
            # take 347 because it is a prime with roughly 49 bins with 7 values.
            sample2 = series.dropna()[::int(len(series.dropna()) / 347) or 1][:347]
            matches.append(filtered([TypeMatch(tm.type, tm.type.match(sample2, 60, 90, previous_match=tm.match)) for tm in matches[-1]]))  #; print len(matches[-1]), matches[-1]
        # See if enough matches found; if any of the group-matches is none, don't do complete match.
        if len(matches[-1]) > 1:
            # Don't take a sample, but do complete comparison between all values (slooow!!)
            matches.append(filtered([TypeMatch(tm.type, tm.type.match(series, 90, 99, previous_match=tm.match)) for tm in matches[-1]]))  # ; print len(matches[-1]),
        # Return the last filled matches, if any.
        return ([tm_list for tm_list in matches if tm_list] or [[]])[-1]
    def verify(self, typename, series):
        """Check if the specified typename still applies to the supplied series. Return bool.
        Developers note: this should NOT be more strict than first step in type-matching, or the 
        found type is rejected by the verification."""
        if typename not in self: return False
        t = self[typename]
        sample = series.dropna()[::int(len(series.dropna()) / 97) or 1]
        return (t.match(sample, 0, 18) < .7).all()
    def __iter__(self):
        self._load()
        for t in self.types.values():
            yield t
    def __contains__(self, key):
        self._load()
        return key in self.types
    def __getitem__(self, key):
        self._load()
        return self.types[key]
    __getattr__ = __getitem__

def test_match():
    import indroid.pandas as bp
    t = Types()
    # Get the data types from an example corpus:
    #df = bp.read_file(r"P:\data\source\kpn\swol_marketing\campagnes\1245 Cloud-event DM\MAM1245.005.csv", encoding='cp1252')
    #df = bp.read_file(r"P:\data\source\kpn\swol_marketing\google\2010-03-23\s1119446a - batch 1 - 50 000 adressen_sbi.csv", encoding='cp1252', nrows=1000)
    df = bp.read_file(r"P:\data\source\kpn\swol_marketing\google\2010-03-23\s1119446a - batch 1 - 50 000 adressen_sbi2.csv", encoding='cp1252')
    for col in df:
        #if not any([v in col.lower() for v in ('fanaam','plts', 'cadres', 'postcode')]):
        #if not any([v in col.lower() for v in ('huisnr', )]):
            #continue
        print col, t.matches(df[col])

def test_match_qb():
    import indroid.pandas as bp
    t = Types()
    df = bp.read_source('odin_link', 'odin_bol')[-1]
    o2i = bp.read_source('odin_link', 'odin_ivp')[-1]
    for col in o2i:
        #if not any([v in col.lower() for v in ('telefoon', 'mobiel')]):
            #continue
        print col, t.matches(o2i[col])

def test_cache():
    for t in Types():
        print t.name, '-'*60
        for kc in sorted(t.corpus.keys()):
            print kc
            c = t.corpus[kc]
            for kcat in sorted(c.categorizers.keys()):
                print kcat
                print c[kcat][:5]

if __name__ == '__main__':
    test_match()
    sys.exit()
    test_cache()
    p = t['nl.postcode']
    print p.validate('1314 p c')
    print p.normalize('11 33 h g')