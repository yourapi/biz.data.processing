"""Extract file with 7z-compression"""
import os, subprocess
from os.path import join, split, splitext, exists

class Extract(object):
    def __init__(self, filename):
        self._filename = filename

