from __future__ import division
from biz import pandas as pd
import weakref

###############################################################################
# Container-classes for several helper-classes
###############################################################################

class PrimusInterPares(object):
    "Aspect-based class which can serve as a container for other, similar instances."
    def __init__(self):
        self._pip_others = {}
    def add(self, *others):
        "Add the metadata from an other object (currently: dataframe) to this metadata."
        def add_one(other):
            if other.pip_id() in self._pip_others:
                return
            self._pip_others[other.pip_id()] = other
        for other in others:
            add_one(other)
        if len(self._pip_others) == 1 and not self.pip_id():
            other = self._pip_others.values()[0]
            if other.pip_id():
                self.assume(other)
    def assume(self, other):
        "Assume the identity of the supplied object, abstract method to be instantiated by descendant."
        raise Exception('Abstract method "assume"')
    def pip_id(self):
        """Return the unique ID for this class. Can be overridden by subclass, often advisable.
        If None, assume not initialized."""
        return id(self)

def test_clean():
    import biz.pandas as bp
    odin = bp.read_source('odin', 'extract/odin_dump\.csv', max_level_date=0, reader_kwargs={'nrows': 10000})[-1]
    o1 = clean_column_names(odin, inplace=False)

if __name__ == '__main__':
    test_clean()
    