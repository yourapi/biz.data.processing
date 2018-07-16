#import numpy as np
#import pandas as pd
import os
from os.path import join, split, splitext
data_root = "X:\\"
data_root = "e:\\Gerard\\Dropbox\\"

class Dir(object):
    def __init__(self, path):
        self.path = path
        self.dirs = [filename for filename in os.listdir(path) if os.path.isdir(join(path, filename))]
    def __getattr__(self, name):
        if name in self.dirs:
            return Dir(join(self.path, name))
        else:
            raise ValueError('No path')
    def __dir__(self):
        return self.dirs

d = Dir(data_root)
data = d.data
