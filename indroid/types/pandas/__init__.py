# Get all top-level classes/functions. import them from  api.py from lower-level modules.
from indroid.pandas.core.api import *
from indroid.pandas.io.api import *
from indroid.pandas.tools.api import *
from indroid.metadata.api import *
from indroid.pandas import monkey
monkey.patch_all()
pandas.computation.expressions.set_use_numexpr(False) # Bug in MKL can crash kernel with large dataframes