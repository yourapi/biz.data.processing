"""Extract file with rar-compression"""
import os, subprocess
from os.path import join, split, splitext, exists
from time import sleep

class new(object):
    def __init__(self):
        pass
    def __call__(self):
        while True:
            sleep(1)

class Extract(object):
    def __init__(self, filename):
        self._filename = filename

