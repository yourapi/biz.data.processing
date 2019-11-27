import numpy as np
from indroid.metadata.types import Types
from urllib import urlencode
from datetime import date, datetime
from dateutil.parser import parse
from collections import defaultdict
from functools import partial
import os, re, sys, copy, hashlib
from indroid import types, pandas as pd, pandas
from os.path import join, split, splitext, splitdrive, isdir, isfile, exists

###############################################################################
# This module contains objects for retrieving data from sources. When data is retrieved
# from a source, the contents are stored in the cache for later fast retrieval.
###############################################################################

def mystat(filename):
    "Return a string representation of the important attributes of a file: size, creation date, change date."
    s = os.stat(filename)
    return "nt.stat_result(st_size={}L, st_mtime={}L, st_ctime={}L)".format(long(s.st_size), long(s.st_mtime), long(s.st_ctime))

class BizHDFStore(pd.HDFStore):
    def __init__(self, filename, mode=None):
        super(BizHDFStore, self).__init__(filename, mode, 9, 'blosc', False)
        self._mode = mode
    def __enter__(self):
        return self
    def __exit__(self, type, value, traceback):
        if self._mode in ('a', 'w', 'r+'): self.flush()
        self.close()
        return True

class Cache(object):
    """pandas.HDFStore is encapsulated in the Cache-object, which is responsible for closing down the h5-file.
    The stored object is always a pandas DataFrame, the returned object is also always a pandas DataFrame.
    Internally a biz DataFrame is used, it is the responsibility of the caller to convert the returned object."""
    def __init__(self, filename, process_name=None):
        self._filename = filename.format(process=process_name) if re.match('.*\{process}', filename) and process_name else filename
        if not os.path.exists(self._filename):
            path = split(self._filename)[0]
            if not exists(path): os.makedirs(path)
            with BizHDFStore(self._filename, 'w'):
                pass
    def key_from_filename(self, filename, **kwargs):
        "Return the filename (without drive) and all kwargs as extra argument with file."
        s_arg = ':' + hashlib.md5(str([(k, kwargs[k]) for k in sorted(kwargs)])).hexdigest() if kwargs else ''
        return splitdrive(filename)[1].replace('\\', '/') + s_arg
    def hash_equal(self, filename, hash, **kwargs):
        result = False
        with BizHDFStore(self._filename, 'r') as hdf:
            attrs = hdf.get_storer(self.key_from_filename(filename, **kwargs)).attrs
            result = ((not hash and not hasattr(attrs, 'hash')) or \
                      (hasattr(attrs, 'hash') and attrs.hash == hash))
        return result        
    def present(self, filename, hash=None, **kwargs):
        """Look up the specified filename and return whether it is in the cache with the present file-attributes.
        The drive-letter of the file is ignored, so the filesystem is portable with regard to drive-letter."""
        result = self.hash_equal(filename, hash, **kwargs)
        if result:
            with BizHDFStore(self._filename, 'r') as hdf:
                attrs = hdf.get_storer(self.key_from_filename(filename, **kwargs)).attrs
                result = hasattr(attrs, 'stat') and attrs.stat == mystat(filename)
        return result
    def retrieve(self, filename, hash=None, **kwargs):
        "Get the contents of the cache, based on the filename. If the filename is not present, raise error."
        result = None
        if not self.hash_equal(filename, hash, **kwargs):
            raise IOError('Hash code for "{}" not correct'.format(filename))
        with BizHDFStore(self._filename, 'r') as hdf:
            result = hdf.get(self.key_from_filename(filename, **kwargs))
        return result
    def store(self, filename, df, hash=None, **kwargs):
        """Put the data in the appropriate place and update the file-stats. Only native DataFrame is supported, 
        so it is temporarily converted to pd.DataFrame and reverted to original type when returned."""
        with BizHDFStore(self._filename, 'a') as hdf:
            try:
                c = df.__class__
                df.__class__= pd.DataFrame
                hdf.put(self.key_from_filename(filename, **kwargs), df, format='table')
                hdf.get_storer(self.key_from_filename(filename, **kwargs)).attrs.stat = mystat(filename)
                if hash:
                    hdf.get_storer(self.key_from_filename(filename, **kwargs)).attrs.hash = hash
                df.__class__ = c
            except Exception as e:
                print e

def path_match(pattern, path, flags=re.I):
    """Return True if the supplied path matches the pattern. The pattern can contain subdirs, 
    where each dir must match the pattern. Subdirs are always separated by forward slashes, to enable escape 
    backslashes for regexes."""
    while pattern:
        pat_parts = pattern.split('/')
        pattern, pat_top = '/'.join(pat_parts[:-1]), pat_parts[-1]
        path, path_top = split(path)
        if not re.match(pat_top, path_top, flags):
            return False
    return True

def levels_path(path):
    "Return the number of levels the path is deep."
    i = 0
    while True:
        path_new, rest = split(path)
        if rest: i += 1
        if path == path_new:
            return i
        path = path_new

def filename_canonical(filename):
    "Return the canonical filename for merging a dataframe. Remove extension, spaces, digits for date-cleansing."
    name = re.sub('[\W\d]', '_', splitext(split(filename)[-1])[0].lower())
    while '__' in name: name = name.replace('__','_')
    name = name.strip('_')
    return name

class Options(dict):
    def __init__(self, data={}, **kwargs):
        #super(Options, self).__init__(lambda: None)
        for k, v in data.items():
            self[k] = v
        for k, v in kwargs.items():
            self[k] = v
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    def update_if_missing(self, other):
        "Set the supplied value if key not present in self, regardless of value of key."
        for k, v in other.items():
            if k not in self:
                self[k] = v
    def update_if_value(self, **kwargs):
        self.update_if_missing(kwargs)
        for k, v in kwargs.items():
            if v is not None:
                self[k] = v

# ToDo: make it possible to read an individual file, to enable mapping. It should be possible to do something like: read_source(filename), without indexing!
class read_source(object):
    """Entry point for data, based on a client and a source. Data can be retrieved, based on index or date.
    The data is cached in an HDF-store and retrieved when requested. The cache can be cleaned up with maintenance-calls.
    Updated data can be stored back in the cache, so typed data (numbers, dates etc) can be stored in the HDF-file directly.
    Storing back data must be designed and implemented.
    Options can be specified on the class-level and are overridden the instance-level options. 
    ToDo: return data for multiple dates; incorrectly now assumed to work as slice!!!"""
    default_options = Options(level_last=None, filename='.*', 
                              data_root=r"P:\data", metadata_root=r"P:\metadata", cache_root=r"P:\cache", 
                              file_cache='{process}.h5', from_cache=True, to_cache=True, level_cache=3, 
                              merge_sources=False, merge_dates=True, pipeline=[], ignore_path_dates=False,
                              reader_kwargs={}, filename_cleaner='filename_canonical')
    def __init__(self, level_last=None, filename=None, levels=None, 
                 data_root=None, metadata_root=None, cache_root=None, 
                 file_cache=None, from_cache=None, to_cache=None, level_cache=None, 
                 merge_sources=None, merge_dates=None, pipeline=None,
                 filename_cleaner=None, max_level_date=None, max_level_file=None, 
                 ignore_path_dates=None, reader_kwargs=None, 
                 options=None, **kwargs):
        """The data-source is specified by giving a data-root and names for the levels. Default 3 levels are used, 
        but a different number can be specified by:
        - specifying the levels as keyword: level0=..., level1=..., leveln=...
        - specifying the levels as a list/tuple or '/'-separated string. When starting with / it is the 
          root of the levels, otherwise it is appended to the possibly other specified levels in keywords.
        The filename can contain regular expressions for matching multiple filenames. Caveat: a match is done, so add $ for end of match!
        A root-starting 'levels'-argument precedes the separate leveln=-keywords.
        The filename may also contain a directory, separated by forward slash: 'extract/.*dump\.csv'
        If the filename is found, all level-indicators are ignored and just the specified file is read.
        Parameters can be set on 3 levels:"""
        from indroid.pandas.core.collect import file_date   # Do it here; at module-level a circular reference is created
        def get_levels(filename, level_last, levels, **kwargs):
            "Return the levels as specified. The levels can be specified by index (starting at 0) and by specifying a sequence of levels."
            if exists(filename):
                # The filename exists; single specified file, look no further.
                path = split(filename)[0]
                if path.lower().startswith(kwargs['data_root'].lower()):
                    path = path[len(kwargs['data_root']):]
                levels = []
                while True:
                    path, branch = split(path)
                    if not branch:
                        break
                    levels = [branch] + levels
                return levels
            levels_add = []
            if not levels:
                levels = []
            elif isinstance(levels, basestring):
                levels = levels.split('/')
                if levels[0]:
                    # String did not start with '/', so is a relative path. Make new levels and add as path:
                    levels_add = levels
                    levels = []
                else:
                    levels = levels[1:]
            for k, v in kwargs.items():
                r = re.match('level(\d+)', k)
                if r and v:
                    lvl = int(r.group(1))
                    while len(levels) <= lvl:
                        levels.append(None)
                    levels[lvl] = str(v)
            levels += levels_add
            levels.append(level_last)
            return [lvl for lvl in levels if lvl]
        options = options or Options()
        options.update_if_missing(self.default_options)
        options.update_if_value(level_last=level_last, filename=filename, levels=levels, data_root=data_root,
                                metadata_root=metadata_root, cache_root=cache_root, file_cache=file_cache, from_cache=from_cache,
                                to_cache=to_cache, level_cache=level_cache, merge_sources=merge_sources, merge_dates=merge_dates,
                                pipeline=pipeline, filename_cleaner=filename_canonical, max_level_date=max_level_date, 
                                max_level_file=max_level_file, ignore_path_dates=ignore_path_dates, reader_kwargs=reader_kwargs,
                                **kwargs)
        self._levels = get_levels(**options)
        self._file_cache = join(options.cache_root,
                                join(*self._levels[:options.level_cache]),
                                options.file_cache)
        self._path = join(options.data_root, *self._levels)  # Must get solution for case-sensitive filesystem like on Linux
        if 'map_columns' in kwargs and kwargs['map_columns'] and 'map' not in options.pipeline: options.pipeline.append('map')
        self._filename_cleaner = eval(options.filename_cleaner) if isinstance(options.filename_cleaner, basestring)  else options.filename_cleaner
        # The filenames are stored by date and index-number. The index here is a multi-index, first date, then an index-number
        # All dates on all (sub-)levels are indexed 
        filenames_by_date = defaultdict(list)
        self.options = options
        if exists(options.filename):
            # The filename exists; single specified file, look no further.
            self._path = split(options.filename)[0]
            filenames_by_date[pd.to_datetime(file_date(options.filename))].append(options.filename)
        elif options.ignore_path_dates:
            # Don't assume path dates, but access the 'raw' path-structure:
            for dirpath1, dirnames1, filenames1 in os.walk(self._path):
                if options.max_level_file is not None and \
                   levels_path(dirpath1) - levels_path(self._path) > options.max_level_file:
                    continue
                for filename1 in filenames1:
                    # ToDo: filter on attributes like hidden etc.
                    if filename1.startswith('~'): continue # Filter Excel-working files
                    full_filename = join(dirpath1, filename1)
                    if path_match(options.filename, full_filename):
                        # Add to dataframe, make index:
                        filenames_by_date[pd.to_datetime(file_date(full_filename))].append(full_filename)
        else:
            # Take path-dates as source for timestamp, regardless of dates of files (standard collection-procedure)
            for dirpath1, dirnames1, filenames1 in os.walk(self._path):
                if max_level_date is not None and levels_path(dirpath1) - levels_path(self._path) > max_level_date:
                    continue
                for dirname1 in dirnames1:
                    r = re.match('.*?(\d+\-\d+\-\d+)', dirname1)  # In the future, add several deliveries on one day? Subdirs/timestamps?
                    if r:
                        try:
                            date = pd.to_datetime(r.group(1))
                        except Exception as e:
                            print e  # ToDo: integrated logging/progress info  etc
                            continue
                        for dirpath2, dirnames2, filenames2 in os.walk(join(dirpath1, dirname1)):
                            if options.max_level_file is not None and \
                               levels_path(dirpath2) - levels_path(join(dirpath1, dirname1)) > options.max_level_file:
                                continue
                            for filename2 in filenames2:
                                # ToDo: filter on attributes like hidden etc.
                                if filename2.startswith('~'): continue # Filter Excel-working files
                                if path_match(options.filename, join(dirpath2, filename2)):
                                    # Add to dataframe, make index:
                                    filenames_by_date[date].append(join(dirpath2, filename2))
        # Now contruct TWO series: a timeseries with the string-date as value, and a multi-index series
        # with the string-date as first key and indexnum as second key. The timeseries can be used as regular date-index.
        self.dates = pd.Series([str(k) for k in filenames_by_date.keys()], filenames_by_date.keys(), name='date').sort_index()
        self._filenames = pd.concat({str(k): pd.Series(filenames_by_date[k], name='filename') for k in filenames_by_date})
        filenames_total = defaultdict(list)
        for l in filenames_by_date.values():
            for filename in l:
                filenames_total[self._filename_cleaner(filename)].append(filename)
        self.filenames_index = pd.concat({k: pd.Series(filenames_total[k], name='filename') for k in filenames_total})
    def __call__(self, **kwargs):
        "Return a new read_source object, based on the current. Supplied arguments override arguments of current read_source."
        options = Options(self.options)
        options.update(kwargs)
        return read_source(options=options)
    @property
    def empty(self):
        """Return a copy of self, which produces empty dataframes, for quick testing of indexes.
        The dataframe contains 1 column and 1 row, with value 1."""
        stub = copy.copy(self)
        stub.from_file = lambda filename, from_cache=True, to_cache=True: pd.DataFrame([{'cnt':1}], [filename[len(self._path)+1:]])
        return stub
    def _nearest_index(self, arg):
        """Return the nearest date for the given argument."""
        # No elegant 'pandas-way' for determining the best fit; time series don't support subtraction from date...
        arg = pd.to_datetime(arg)  # Let pandas handle all conversions
        # Find the nearest date, based on distance to index of dates:
        i = self.dates.index[0]
        d = abs(i - arg)
        for i1 in self.dates.index:
            if abs(arg - i1) < d:
                d = abs(i1 - arg)
                i = i1
        return i
    def __iter__(self):
        return self[:]
    def __getitem__(self, key):
        """Get an item based on index or date. When a date is specified and the exact date does not exist,
        the closest source is selected. The semantics are completely identical to time series retrieving in pandas.
        Always return a dataframe with the data; when multiple sources are present, the index is a multi-index with 
        the complete filename as extra level. When more than one date is returned, add an additional level with the date."""
        from indroid.pandas.tools.merge import concat
        date_select, file_select = key if isinstance(key, tuple) else (key, None)
        def multiple_files(filenames, file_select):
            if file_select is None: return filenames
            if isinstance(file_select, basestring):
                return [filename for filename in filenames if re.match(file_select, self._filename_cleaner(filename), re.I)]
            if isinstance(file_select, (list, tuple, set)):
                return [filename for filename in filenames if any([re.match(file_select1, self._filename_cleaner(filename), re.I) for file_select1 in file_select])]
            if isinstance(file_select, slice):
                if isinstance(file_select.start, basestring) and isinstance(file_select.stop, basestring):
                    # Start and stop are string, get all sources between these borders (inclusive):
                    if file_select.step not in (-1, None, 1):
                        raise ValueError('No other stepping allowed than -1, None or 1, found "{}"'.format(file_select.step))
                    if file_select.step == -1:
                        file_select = slice(file_select.stop, file_select.start, -file_select.step)
                    return [filename for filename in filenames if file_select.start.lower() <= self._filename_cleaner(filename) <= file_select.stop.lower()]
                else:
                    # No use, but return anyway. Sort by cleansed filename:
                    return sorted(filenames, key=lambda f: self._filename_cleaner(f))[file_select]
        def multiple_dates(date_select, file_select):
            "The key is the selecting key; can be slice or string (for date-slicing). When it is a string, it has proven to return multiple dates."
            try:
                dates = self.dates[date_select]
            except KeyError:
                return pd.DataFrame()
            result = {}
            # Get all dates and all sources for the dates:
            for date in dates.values:
                dataframes = {self._filename_cleaner(filename): self.from_file(filename) for filename in \
                              multiple_files(self._filenames.loc[date].values, file_select)}
                result[date] = concat(dataframes) if self.options.merge_sources else dataframes
            # Now see how much the individual frames should be concatenated:
            if self.options.merge_sources:
                # Every date contains one dataframe, concat it or not:
                if len(result) == 0:
                    return pd.DataFrame()
                elif len(result) == 1:
                    return result.values()[0]
                else:
                    return concat(result) if self.options.merge_dates else result
            elif self.options.merge_dates:
                # Merge the dates, but keep the files! New dataframe with merged dates per file.
                # The filename is made canonical with default canonical-function.
                dataframes = defaultdict(dict)
                for date, frames in result.items():
                    for filename, frame in frames.items():
                        dataframes[filename][date] = frame
                result1 = {filename: concat(frames) for filename, frames in dataframes.items()}
                return result1.values()[0] if len(result1) == 1 else pd.DataFrame() if len(result1) == 0 else result1
            else:
                # If no dates merged, return individual dataframes:
                return result.values()[0] if len(result) == 1 else pd.DataFrame() if len(result) == 0 else result
        # Take two different routes, based on slice- or mono-indexing:
        if isinstance(date_select, slice):
            return multiple_dates(date_select, file_select)
        else:
            # Single key, try to get it. Can result in multiple result (string-based date-selection, like '1999')
            try:
                dates = self.dates[date_select]
            except KeyError:
                dates = self.dates[self._nearest_index(date_select)]
            # dates can be single value (string, point into one delivery), or multi:
            if isinstance(dates, pd.Series) and len(dates) == 1:
                # Single date found. Return delivery for this date only:
                dates = dates.values[0]
            if isinstance(dates, basestring):
                # One delivery; can be single or multi-file:
                # Return a dataframe with filename as index:
                dataframes = {self._filename_cleaner(filename): self.from_file(filename) for filename in multiple_files(self._filenames.loc[dates].values, file_select)}
                if len(dataframes) == 0:
                    return pd.DataFrame()
                elif len(dataframes) == 1:
                    return dataframes.values()[0]
                elif self.options.merge_sources:
                    # ToDo: make useful combination of metadata and processor:
                    return concat(dataframes)
                else:
                    return dataframes
            else:
                # Multiple deliveries; same as slicing:
                return multiple_dates(date_select, file_select)
    def from_file(self, filename):
        """Get the data for a specified date. Check presence of content in cache and on disk, if both are present,
        return content in cache if attributes of file are not changed and the from_cache-flag is set.
        If content not in cache or cache is outdated, update the cache if to_cache-flag is set.
        CAVEAT: when storing data in the cache, it is stored in the default encoding (cp1252, now a constant).
        Unicode is NOT supported by pytables, the underlying storage-package."""
        import  indroid.pandas as bp
        # Get the total cache-table with filenames and store-data. If the cache gets very large, selection and 
        # updating of table on disk should be enabled (ToDo)
        df = bp.DataFrame()
        # Assign meta-data to dataframe, for identification of source etc.
        df._biz_metadata.set(self._levels, self.options.metadata_root, self._filename_cleaner(filename))
        df._biz_processor.set(filename, self.options.reader_kwargs, self.options.pipeline, self._file_cache, self.options.from_cache, self.options.to_cache)
        df._biz_processor.process()
        return df

def test_ip():
    import indroid

    src = indroid.pandas.read_source(levels='/source/kpn/swol_marketing', pipeline=['input', 'map'], to_cache=False, merge_sources=True)
    sent = src(level_last='tripolis', filename='zm_.*\.txt$', level0='export', max_level_date=0)['2012-12':]    
    
    ip = read_source('', file_mask='extract/.*', project='data', client='vol', data_root='P:\\', reader_kwargs={'nrows':1000, 'encoding': 'mbcs'}, merge_sources=False, merge_dates=True, max_level_date=0)
    #print ip['2013-09', '^ft$']
    #print ip['2013-09', 'baan$']
    #print ip['2013-09', 'baan']
    print ip['2013-03':'2013-05', 'cadfast':'cadmlowifi']
    print ip['2013'::4, 2:7:2]
    print ip['2013-09', ['cadmlo$', 'ipm_mlo', '^mlo$']]
    for merge_sources in False, True:
        for merge_dates in False, True:
            print read_source('batches', project='ip_cleaner', client='data', data_root='P:\\vol', reader_kwargs={'nrows':1000, 'encoding': 'mbcs'}, merge_sources=merge_sources, merge_dates=merge_dates, max_level_date=0)['2013-09-23':'2013-09-27']    
    for merge_sources in False, True:
        for merge_dates in False, True:
            print read_source('', file_mask='extract/.*', project='data', client='vol', data_root='P:\\', reader_kwargs={'nrows':1000, 'encoding': 'mbcs'}, merge_sources=merge_sources, merge_dates=merge_dates, max_level_date=0)['2013']
    return
    for merge_sources in False, True:
        for merge_dates in False, True:
            print read_source('', client='vol', project='data', data_root='P:\\', reader_kwargs={'nrows':1000, 'encoding': 'mbcs'}, merge_sources=merge_sources, merge_dates=merge_dates)['2013-05']

def test_outbound():
    def filename_outbound(filename):
        "Return the canonical filename for outbound, the numbers preceding the extension are significant."
        name = re.sub('[\W\d]', '_', splitext(splitext(split(filename)[-1])[0])[0].lower())
        while '__' in name: name = name.replace('__','_')
        name = name.strip('_') + splitext(splitext(filename)[0])[1]
        return name
    outbound = read_source('outbound', '.*\.csv',data_root=r"P:\data\analysis", merge_sources=False, 
                           filename_cleaner=filename_outbound)
    print outbound.empty[-2:]
    print outbound.empty[-2:,'swol_outbound.10']
    print outbound.empty[-5:,'swol_outbound.104']
    print outbound.empty[-5:,'swol_outbound_opzeg']
    o = outbound[-2:]
    opz = outbound['2013-10-02','swol_outbound_opzeggers']

def test_cam():
    cam = read_source('cam', 'PCS Almere/Respons/Verwerkte respons/1327_.*')

if __name__ == '__main__':
    # ToDo: get config for filenames in subdirs
    #src = read_source(levels='/source/kpn/swol_marketing', pipeline=['input'], merge_sources=True)
    #factuur = src(level_last='exact', ignore_path_dates=True)['2008-04-01', 'factuurbestand']
    
    #test_ip()
    #sys.exit()
    #e = read_source(filename=r"P:\data\download\reportexport (5).csv", map_columns=True)[0]
    #src = read_source(levels='/source/kpn/swol_marketing', pipeline=['input', 'map'], merge_sources=True, from_cache=False)
    #sent = src(level_last='tripolis', filename='zm_.*\.txt', level0='export')['2013-07':'2014-04']

    s0 = read_source(filename=r"C:\Users\gerar_000\Downloads\exportContactsThatOpened.csv", pipeline=['input', 'map'])[0]
    print len(s0)
    
    s1 = read_source(levels='/source/foo/bar', pipeline=['input', 'map'])
    print s1.empty[-1:, 'baz']
    baz = s1(from_cache=False)[-1, 'baz']

    s1 = read_source(levels='/vol/data', data_root='P:\\', pipeline=['input', 'map'], from_cache=True)
    print s1.empty[-2:, 'ipm_fcode']
    ipm = s1[-2:, 'ipm_fcode']

    src = read_source(levels='/source/kpn/swol_marketing', pipeline=['input', 'map'], merge_sources=True)
    
    others = src(level_last='tripolis', filename='ZM_.*.txt', level0='export',from_cache=False)['2014':]
    # Test application of higher order functions on dataframe.
    others = others()
    #------------------------
    read_source.default_options.pipeline = ['input', 'map']
    read_source.default_options.levels = '/source/kpn/swol_marketing'
    o1 = read_source(filename=r"P:\data\download\querybuilder_20140216123534.csv", from_cache=True)
    o2 = o1(from_cache=False)
    print o1.empty[0], o2.empty[0]
    previous = read_source('tripolis', 'ZM_IVP.*.txt', level0='export',  merge_sources=True)[-1]
    s0 = read_source(filename=r"P:\data\download\reportexport (3).csv")
    print s0.empty[0]
    df = s0[0]
    s2 = read_source('odin', 'extract/odin_dump\.csv', max_level_date=0)
    print s2.empty['2013-10']
    odin1 = s2['2013-10']
    s3 = read_source('odin', 'extract/odin_dump\.csv', max_level_date=0)
    odin2 = s3[-1]
    sys.exit()
    test_outbound()
    odin = read_source('odin', 'odin_dump\.csv', merge_sources=False, reader_kwargs={'nrows':1000, 'encoding': 'cp1252'})
    #print odin[-3]
    #print odin[-1]
    #print odin[-10]
    import indroid.pandas as bp
    o = odin['2013-9':'2013-10']
    #o = odin['2013-10']
    o[o.org_name.fillna('').str.contains('caf', flags=re.I)].org_name
    print odin['2013-11']    
    print odin[datetime(2013, 10, 1, 15, 45)]
    print odin['2013-11':'2014-11':2]
    print odin['2013-9':'2014-11':2]
    print odin['2013-1-1':'2013-12-31':4]
    print odin['2013-01-01':'2013-07-01':4]
    print odin[date(2013,9,25)]
    #print  odin[-1]
    iaso = read_source('iaso', '.*repaired.*\.xml')
    o = odin[-20:]
    i = iaso[-20:]
    print o
    print i