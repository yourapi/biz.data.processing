"""Extract file with zip-compression"""
import os, subprocess
from os.path import join, split, splitext, exists

class Extract(object):
    def __init__(self, filename):
        self._filename = filename

