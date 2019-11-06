#!/usr/bin/env python

"""Module for reading csv-like files. A number of classes are present for types and typeless (string-only)
generation of data.
ToDo: generate Unicode-values only and correctly handle input-files """
from __future__ import absolute_import  # Make sure that system-modules are loaded by default
import os, sys
if os.getcwd() in sys.path:
    sys.path.remove(os.getcwd())  # Do not import local csv-module by accident!
import csv, codecs, cStringIO, chardet
from collections import OrderedDict
from biz.input.csv_dialects import *

class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f, errors='replace')
        # Sometimes BOF-character is not consumed rightly, check it here.More characters might include:
        # ['\xef\xbb\xbf', '\xff\xfe\x00\x00', '\x00\x00\xfe\xff', '\xfe\xff\x00\x00', '\x00\x00\xff\xfe']
        if self.reader.read(1) not in (u'\ufeff', u'\ufffe'):
            self.reader.seek(0)

    def __iter__(self):
        return self

    def next(self):
        line = u''
        try:
            line = self.reader.next().encode('utf-8')
            return unicode(line)
        except UnicodeError:
            return line

class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """
    def __init__(self, file_or_filename, dialect=None, encoding=None, 
                 dialect_confidence_threshold=.5, encoding_confidence_threshold=.5, 
                 encoding_preferences=('1252', '8859-1'), encoding_heuristic_length=50*2**10, **kwds):
        f = file_or_filename if isinstance(file_or_filename, file) else open(file_or_filename, 'rb')            
        if encoding is None:
            encoding = self.encoding_heuristic(f, encoding_confidence_threshold, 
                                               encoding_preferences, encoding_heuristic_length)
        else:
            self.encoding_confidence = 2.0
        if dialect is None:
            dialect = self.dialect_heuristic(f, dialect_confidence_threshold)
        else:
            self.dialect_confidence = 2.0
        self.encoding = encoding
        self.dialect = dialect
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)
    def encoding_heuristic(self, stream, confidence_threshold, 
                           encoding_preferences=('1252', '8859-1'), length=50*2**10):
        "encoding_preferences overrule other code-pages to prevent mistakes. Should be changed in other countries than NL!!!"
        cd = chardet.detect(stream.read(length)) # Read 100k of data, should be enough :-)
        stream.seek(0)
        confidence, encoding = cd['confidence'], cd['encoding']
        if confidence < confidence_threshold + .5 * (1 - confidence_threshold) and \
           (any([base in encoding.lower() for base in ('windows', 'iso')]) and not
            any([cp in encoding for cp in encoding_preferences])):
            # Heuristic: ANSI (mbcs) is much more probable than exotic windows/iso encoding:
            encoding = 'mbcs'
        if encoding == 'ascii':
            # Plain ASCII is very improbable. Windows is superset of ascii, so does not hurt.
            encoding = 'mbcs'
        if confidence < confidence_threshold:
            cd['threshold'] = confidence_threshold
            raise ValueError('Heuristic determination of encoding failed: {confidence:.1%} confidence in "{encoding}", {threshold:.1%} required.'.format(**cd))
        self.encoding_confidence = cd['confidence']
        return encoding
    def dialect_heuristic(self, stream, confidence_threshold, line_count=100, 
                          dialects=(DialectCSV, DialectComma, DialectTab, DialectPipeNoEscape)):
        # Get the first 100 lines and get the delimiter with the most occurrences and the least std dev:
        # Get first count lines:
        lines = []
        for i, line in enumerate(stream):
            if i >= line_count:
                break
            lines.append(line)
        stream.seek(0)
        if not lines:
            self.dialect_confidence = 0.0
            return dialects[0]
        results = []
        for dialect in dialects:
            nums = [line.count(dialect.delimiter) for line in lines]
            avg = float(sum(nums)) / len(nums)
            stddev = (sum([(num - avg) ** 2 for num in nums]) / len(nums)) ** .5
            if avg >= 1 and stddev / avg < 1 - confidence_threshold:
                results.append((avg, 1 - stddev / avg, dialect))
        results.sort(key=lambda t: t[0], reverse=True)
        if results:
            self.dialect_confidence = results[0][1]
            return results[0][-1]
        else:
            self.dialect_confidence = 0.0
            return dialects[0]
    def next(self):
        row = self.reader.next()
        return [unicode(s, "utf-8") for s in row]
    def __iter__(self):
        return self

