"""Mail objects for reading a specified mail-message and returning the contents, including attachments."""
import os, subprocess
from os.path import join, split, splitext, exists

class FTP(object):
    def __init__(self, address):
        self._address = address

