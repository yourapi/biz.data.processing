from __future__ import division
import numpy as np
from biz import pandas as pd
import sys, re, collections, os, os.path, importlib, weakref, copy
from collections import defaultdict
from isounidecode import unidecode
from biz.pandas.tools.container import PrimusInterPares
from biz.pandas.core.utils import mkdir

###############################################################################
# This module contains helper-functions which all work on a pandas DataFrame.
# All functions have a dataframe as their first argument and in may cases do nothing more than 
# enhance existing functions to make deialing with datafranmes even easier.
###############################################################################

def clean_column_names(df, clean=True, make_unique=True, inplace=True):
    """Replace all column names with names which can be used as property names,
    e.g. replacing a space with an underscore. All letters are converted to lower case:
    Last Name --> last_name
    Zip-Code ; Zip Code  --> zip_code_1 ; zip_code_2  (if make_unique is True)
    """
    def cleaner(name):
        name = re.sub('[\s!@#$%^&\*(\)\.:";''/?\\\\|<>\[\]~\-=]', '_', unidecode(name).strip().lower())
        while '__' in name: name = name.replace('__', '_')
        if re.match('\d.*', name): name = '_' + name
        return name
    if not inplace: df = df.copy()
    names = [cleaner(name) if clean else name for name in df.columns]
    if make_unique:
        counts = collections.Counter(names)
        result = []
        for name in reversed(names):
            if counts[name] > 1:
                suffix = '{}'.format(counts[name])
                if not name.endswith('_'): suffix = '_' + suffix
                while name + suffix in counts:
                    suffix += 'a'
                result.append(name + suffix)
                counts[name] -= 1
            else:
                result.append(name)
        names = list(reversed(result))
    df.columns = names
    return df

def kaplan_meier(survivors, deaths):
    """The survivors is a pandas Series with as index the time and as value the number of known survivors up to that moment.
    The deaths is a pandas Series with as index the time and as value the number of known deaths at that moment.
    The return value is a pandas Series with the combined time as index and the survival rate [0,1] as values."""
    # First version is literal translation of linear KM-estimator from previous code
    df = pd.concat({'deaths': deaths, 'survivors': survivors}, axis=1)
    total = df.sum().sum()
    df['d_sum'] = df.deaths.fillna(0).cumsum()
    df['s_sum'] = df.survivors.fillna(0).cumsum()
    df['remain'] = (total - df.d_sum - df.s_sum).shift(1)
    df['factor'] = (df.remain - df.deaths) / df.remain
    return df.factor.fillna(1).cumprod()

class PipelineProcess(object):
    def __init__(self, modulenames=['biz.pipeline']):
        "Get all processes from the selected modules. "
        self.functions = {}
        for name in modulenames:
            importlib.import_module(name)
            module = sys.modules[name]
            for k, v in module.__dict__.items():
                if not k.startswith('__') and type(v).__name__ == 'function':
                    # Function-type, not starting with __:
                    self.functions[k] = v
    def process(self, name, df):
        return self.functions[name](df)
    def __call__(self, name, df, suffix):
        n = name + suffix
        return self.functions[n](df) if n in self.functions else None        
    def changed(self, name, df):
        return self(name, df, '_changed')
    def hash(self, name, df):
        return self(name, df, '_hash')

class DataframeProcessor(PrimusInterPares):
    """Processor for dataframe which is read from source. Contains all metadata about the dataframe and
    can perform all necessary tasks for 'working up' dataframe to a usable format. """
    pipeline_process = PipelineProcess()
    def __init__(self, df):
        super(DataframeProcessor, self).__init__()
        self.df = weakref.ref(df)  # Store weak ref to enable deletion of container dataframe
        self.dirty = defaultdict(lambda: True)  # Indicates if the specified process is dirty. Set by calling process.
        # The default value for dirty is True,so it is the responsibility of the caller to set it False, if appropriate.
        self.pipeline = []
        self.filename = None
        self.reader_kwargs = None
        self.file_cache = None
        self.from_cache = None
        self.to_cache = None
    def copy(self):
        return copy.copy(self)
    def pip_id(self):
        return self.filename
    def assume(self, other):
        assert isinstance(other, DataframeProcessor)
        self.set(other.filename, other.reader_kwargs, other.pipeline, other.file_cache, other.from_cache, other.to_cache)
    def set(self, filename, reader_kwargs, pipeline, file_cache, from_cache=True, to_cache=True):
        self.filename = filename
        self.reader_kwargs = reader_kwargs
        self.add_pipeline(pipeline)
        self.file_cache = file_cache
        self.from_cache = from_cache
        self.to_cache = to_cache
        self.add(self)
    def add_pipeline(self, pipeline, skip_double=True):
        """Add all steps in the supplied pipeline to the current pipeline.
        If skip_double, the steps which are already present are skipped so each 
        step is only executed once"""
        self.pipeline += [s for s in pipeline if s not in self.pipeline] if skip_double else pipeline
    def process(self):
        # ToDo: step until first dirty step, or last step. The get df from cache!
        from biz.pandas import Cache
        if self.df() is None: return  # weak ref returned when calling it. Exit when no longer present.
        df = self.df()
        prev_step = None
        prev_cache = None
        broken = False
        for step in self.pipeline:
            # Get from cache if present; read from file if:
            # - file different from cache
            # - function changed
            # - Special function present with same name as process, suffixed by _changed
            cache = Cache(self.file_cache, step)
            hash = self.pipeline_process.hash(step, df)
            if not self.from_cache or not cache.present(self.filename, hash=hash, **self.reader_kwargs) or \
               self.pipeline_process.changed(step, df) or broken:
                if not broken and prev_cache:
                    # Data must be retrieved; get previous data if present:
                    df._data = prev_cache.retrieve(self.filename,  hash=prev_hash, **self.reader_kwargs)._data
                df._data = self.pipeline_process.process(step, df)._data
                hash = self.pipeline_process.hash(step, df)
                if self.to_cache and not cache.present(self.filename, hash=hash, **self.reader_kwargs):
                    cache.store(self.filename, df, hash=hash, **self.reader_kwargs)
                broken = True
            prev_step = step
            prev_cache = cache
            prev_hash = hash
        if not broken and prev_cache:
            # Data must be retrieved; get previous data if present:
            df._data = prev_cache.retrieve(self.filename,  hash=prev_hash, **self.reader_kwargs)._data
        # Now assign the data to the container and reset the cache:
        self.df()._data = df._data
        self.df()._clear_item_cache()

class DataFrame(pd.DataFrame):
    def __init__(self, data=None, index=None, columns=None, dtype=None, copy=False):
        """Init by calling super-init and setting provate attributes.
        IMPORTANT: ALWAYS prefix attributes with _biz_, a nameclash occurred with _metadata (without the _biz...)"""
        from biz.metadata.core import Metadata
        super(DataFrame, self).__init__(data, index, columns, dtype, copy)
        self._biz_processor = DataframeProcessor(self)
        self._biz_metadata = Metadata()
    def copy(self):
        df = DataFrame(self._data)
        df._biz_metadata = self._biz_metadata.copy()
        df._biz_processor = self._biz_processor.copy()
        return df
    def assume_metadata(self, other):
        self._biz_metadata = other._biz_metadata.copy()
        self._biz_processor = other._biz_processor.copy()
        return self
    __new__ = object.__new__  # Instantiate parent-object of parent-object, because parent-object instantiates this class. object is first class with __new__ method; other ancestors don't define it.
    def _biz_process(self, *args, **kwargs):
        """Apply the supplied function(s) to the dataframe. The function(s) are specified as separate args and
        executed in the specified order."""
        df = self.copy()
        df._biz_processor.add_pipeline(args, False)
        df._biz_processor.process()
        return df
    __call__ = _biz_process
    def to_csv(self, path_or_buf, sep=";", na_rep='', float_format=None,
               cols=None, header=True, index=False, index_label=None,
               mode='w', nanRep=None, encoding='utf-8', quoting=None,
               line_terminator='\n', chunksize=None,
               tupleize_cols=True, **kwds):
        """Modification of some default arguments: sep=';', index=False
        Creation of destination-path if it doesn't exist."""
        mkdir(path_or_buf)
        super(DataFrame, self).to_csv(path_or_buf, sep=sep, na_rep=na_rep, float_format=float_format,
               columns=cols, header=header, index=index, index_label=index_label, mode=mode, nanRep=nanRep, 
               encoding=encoding, quoting=quoting, line_terminator=line_terminator, chunksize=chunksize, 
               tupleize_cols=tupleize_cols, **kwds)
#DataFrame.__doc__ = """Substitute for standard DataFrame with extra functions (metadata, ...) and some adapted 
    #methods with adapted functionality and/or adapted default arguments.\n""" + pd.DataFrame.__doc__

def test_clean():
    class DF(object): pass
    df = DF()
    df.columns = [u'UPPER CASE', 'Name', '  Leading & TrailING WHITE SPACE!!!  ', '4 a ver\\|/ l-a-r-g-e @#$%^!*()))_', 'NaMe!', 
                  '   NaMe   2', 'Name   2', 'Name 2 & 1', 'Name', 'Name    1', 'col', 'col', 'col', 'col', 'col_2', 'col_2a', 'col_2', 'col_2', 'col_2aa']
    clean_column_names(df)
    print df.columns
    assert max(collections.Counter(df.columns).values()) == 1
def test_clean():
    import biz.pandas as bp
    odin = bp.read_source('odin', 'extract/odin_dump\.csv', max_level_date=0, reader_kwargs={'nrows': 10000})[-1]
    o1 = clean_column_names(odin, inplace=False)

if __name__ == '__main__':
    test_clean()
    