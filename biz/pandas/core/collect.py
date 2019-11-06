from biz import pandas as pd
from biz.pandas.core.cache import path_match
from biz.pandas import read_file
from isounidecode import unidecode
from urllib import urlencode
from datetime import date, datetime
from dateutil.parser import parse
from collections import defaultdict
from functools import partial
import os, re, sys, logging, shutil, time, base64, pyDes
import subprocess
from os.path import join, split, splitext, splitdrive, isdir, isfile, exists, isabs
from zipfile import ZipFile
log = logging.getLogger('collect')
KEY = '\d{2,4}\-\d{1,2}\-\d{1,2'  # 24 characters, not strong, but code is hidden enough... Key MUST BE 16 or 24 characters long
# The key is used for encrypting and decrypting passwords for secure access, e.g. encrypted zip-files.

###############################################################################
# This module contains objects for collecting files from a source to a standard destination.
# When appropriate, contents are extracted from a zip- or other format.
###############################################################################

def encrypt(value):
    return base64.b64encode(pyDes.triple_des(KEY, padmode=pyDes.PAD_PKCS5).encrypt(value))

def decrypt(value):
    return pyDes.triple_des(KEY, padmode=pyDes.PAD_PKCS5).decrypt(base64.b64decode(value))

def decrypt_password(password):
    """Return a list of decrypted passwords for the supplied password(s). When a comma-separated list
    is supplied, a list of decrypted passwords is returned. When a list is supplied, the contents are decrypted."""
    def helper(password):
        if isinstance(password, basestring):
            if ',' in password:
                return [helper(p) for p in password.split(',')]
            else:
                return decrypt(password)
        elif isinstance(password, (list, tuple)):
            return [helper(p) for p in password]
        else:
            return password
    result = helper(password)
    return result if isinstance(result, (list, tuple)) else [result]

def file_date(filename):
    """Return the datestamp of the file. First get a datestamp in the name (yy-mm-dd), then a timestamp (epoch-seconds),
    if they all fail, return the creation-date. Currently, don't consider the containing directories."""
    def date_from_string(s):
        for pattern, minyears in (('\d{2,4}\-\d{1,2}\-\d{1,2}', 15), ('\d{8}', 5)):
            for match in re.findall(pattern, s):
                try:
                    d = parse(match)
                    if not -365 < (datetime.now() - d).days < minyears * 365: continue
                    return d.strftime('%Y-%m-%d')
                except ValueError:
                    pass # No valid date, despite seemingly valid date
        return None
    dirpath, filename = split(filename)
    if date_from_string(filename):
        return date_from_string(filename)
    # Now try timestamp in seconds since epoch:
    for match in re.findall('\d+', filename):
        if len(match) == 10:
            try:
                d = datetime.fromtimestamp(int(match))
                if not -365 < (datetime.now() - d).days < 5 * 365: continue
                return d.strftime('%Y-%m-%d')
            except ValueError:
                pass
    # No valid timestamp in filename. Now try upper directories:
    dirpath1 = dirpath
    while True:
        dirpath1, dirtop = split(dirpath1)
        if not dirtop: break
        if date_from_string(dirtop):
            return date_from_string(dirtop)
    # No valid timestamp in filename or path. Now get modification date:
    try:
        log.info('Filename has no timestamp in name: {}'.format(join(dirpath, filename)))
        d = datetime.fromtimestamp(os.stat(join(dirpath, filename)).st_mtime)
        return d.strftime('%Y-%m-%d')
    except:
        log.warn('Filename has no valid timestamp: {}'.format(join(dirpath, filename)))
        return None

def file_match(source_name, filename):
    if isinstance(source_name, basestring):
        return path_match(source_name, filename)
    elif callable(source_name):
        return source_name(filename)
    elif isinstance(source_name, (list, tuple)):
        return any([file_match(name, filename) for name in source_name])
    else:
        return False

def file_time(filename):
    "Return the modification-time of the file, 6 digits: HHMMSS"
    return datetime.fromtimestamp(os.stat(filename).st_mtime).strftime('%H%M%S')

def stats_equal(file1, file2):
    """Check if the specified files exist and have the same modification time and size.
    For NT,the max delta in modification time is 0.001s, should also be enough for linux,
    depending on resolution of datetime in file-attributes."""
    if not (exists(file1) and exists(file2)):
        return False
    stat1, stat2 = os.stat(file1), os.stat(file2)
    return abs(stat1.st_mtime - stat2.st_mtime) <= .001 and stat1.st_size == stat2.st_size

class CompressedFile(object):
    "Extracting standard compression-files, like zip, 7z etc."
    def __init__(self, filename, target_path):
        self._filename = filename
        self._target_path = target_path
    def extract(self, passwords=[]):
        """Extract the specified filename, using the supplied password(s). Every password is used, 
        including no password."""
        if not None in passwords:
            passwords = [None] + passwords
        # Create target directory :
        target = self._target_path if isabs(self._target_path) else join(split(self._filename)[0], self._target_path)
        returncode = -1
        for pwd in passwords:
            if callable(pwd):
                pwd = pwd(self._filename)
            returncode, err = self.extract_generic(self._filename, pwd)
            if returncode == 0: 
                break
        if returncode:
            log.error('Extraction of "{}" not succeeded, returncode {}: "{}"'.format(self._filename, returncode, err))
    def extract_generic(self, filename, password):
        target = join(split(filename)[0] if not isabs(self._target_path) else '', self._target_path)
        password = password or '' # Ensure empty string for passing to 7zip
        cmd = [r"C:\Program Files\7-Zip\7z.exe", 'x', '-y', '-p'+ password, '-o' + target, filename]
        try:
            result = subprocess.check_output(cmd, stderr=subprocess.STDOUT)        
        except Exception as e:
            # Something went wrong with call. 
            return e.returncode, e.output
        if 'Everything is Ok' in result:
            return 0, result
        else:
            return -1, result

def decompress(filename, target_path, password):
    CompressedFile(filename, target_path).extract(password)

class collect_source(object):
    """Entry point for collecting files and copying/moving them to their target destination.
    The collected files are compared to a destination file (if present) and copied if not present or 
    no exact match on file attributes."""
    def __init__(self, source_path, source_name, dest_name=None, fieldnames=None, name=None, project='swol_marketing', 
                 client='kpn', data_root=r"P:\data\source", extract_path='', extract_password=None,
                 extract=('\.zip', '\.7z')):
        """source_path: the path to look; may be list with several paths to look or a function.
        source_name: the single name or multiple names of the file(s) to match. Are matched with regex. Can also be a function which is fed the full file name
        The source name may refer to a zipped file, when more filenames are specified, they are matched to the
        contents of the zip-file: ('download.zip', 'data\d+.csv', 'log.*.txt') matches the filenames 
        data123.csv and log_web.txt within download.zip. Extra arguments for unzipping (like password) can be 
        given in **kwargs. Password (or list of passwords) is encrypted."""
        def fieldname_match(fieldnames, filename):
            if not fieldnames:
                return True
            try:
                df = read_file(filename_full, nrows=1, encoding='cp1252') # Speed up parsing by assuming cp1252. It is VERY unusual to have special characters in fieldname, let alone outside cp1252 for Europe. Guessing the encoding for every file is VERY time-consuming.
                fieldnames_src = df.columns
            except:
                try:
                    fieldnames_src = [open(filename_full).readline()]
                except:
                    fieldnames_src = ['']
            return all([ \
                any([re.match(fieldname_target, fieldname, re.I) for fieldname in fieldnames_src]) \
                for fieldname_target in fieldnames])
        self._source_path = [source_path] if isinstance(source_path, basestring) else source_path
        self._source_name = source_name
        self._dest_name = dest_name
        self._fieldnames = [fieldnames] if isinstance(fieldnames, basestring) else fieldnames
        self._name = name
        self._project = project
        self._client = client
        self._data_root = data_root
        self._destination_path = join(data_root, client, project, name)  # Must get solution for case-sensitive filesystem like on Linux
        self._extract_path = extract_path
        self._extract_passwords = decrypt_password(extract_password)
        self._extract = extract
        self._filenames = []
        # Get all appropriate files and see if they're new:
        for path in self._source_path:
            if not (exists(path) and isdir(path)):
                log.warn('Path does not exist: {}'.format(path))
            for dirpath, dirnames, filenames in os.walk(path):
                for filename in filenames:
                    filename_full = join(dirpath, filename)
                    if file_match(source_name, filename_full):
                        # See if fieldnames present, if specified. If not, continue to next filename
                        if fieldname_match(self._fieldnames, filename_full):
                            # All conditions are fulfilled. See if the file doesn't already exist:
                            file_dest = self.destination_file(filename_full)
                            if not stats_equal(filename_full, file_dest):
                                self._filenames.append(filename_full)
    def decompress_if_possible(self, filename):
        ext = splitext(filename)[-1]
        if any([re.match(ext1, ext, re.I) for ext1 in self._extract]):
            decompress(filename, self._extract_path, self._extract_passwords)
    def copy(self, filenames=None):
        """Copy the collected files to their destination. If a conflicting , different, file exists, rename it 
        according to its creation date. The supplied filenames act as a filter for downloading files."""
        for filename in self._filenames:
            if filenames and not file_match(filenames, filename):
                continue
            file_dest = self.destination_file(filename)
            try: os.makedirs(split(file_dest)[0])
            except WindowsError: pass
            shutil.copy2(filename, file_dest)
            # If the file is compressed (as indicated by its extension), decompress it:
            self.decompress_if_possible(file_dest)
            # Now check if a filename with timestamp was given and a 'bare' filename exist
            # If a bare name exists, rename it:
            r = re.match('(.*)\.\d{6}(\..*)', split(file_dest)[-1])
            if r:
                filename_root = r.group(1) + r.group(2)
                if exists(join(split(file_dest)[0], filename_root)):
                    # The name exists; rename the root-filename to include time-stamp:
                    filename_root_new = r.group(1) + '.' + file_time(join(split(file_dest)[0], filename_root)) + r.group(2)
                    if exists(join(split(file_dest)[0], filename_root_new)):
                        os.remove(path)
                    os.rename(join(split(file_dest)[0], filename_root),
                              join(split(file_dest)[0], filename_root_new))
    def destination_file(self, filename):
        "Generate the destination-file, based on the source-name, destination-path and destination-name."
        path = join(self._destination_path, file_date(filename))
        dest_name = split(filename)[-1] if self._dest_name is None else self._dest_name(filename) if callable(self._dest_name) else \
            self._dest_name if '.' in self._dest_name else self._dest_name + splitext(filename)[-1]
        # Now it gets tricky. If destination file with different attributes exists, generate name with time of date-creation as unique sub-index.
        # When A LOT of files are generated every day, could become bottleneck. For now, hypothetical case :-)
        file_dest_single = join(path, dest_name)
        if not exists(path):
            return file_dest_single
        file_dest_multi = splitext(file_dest_single)[0] + '.' + file_time(filename) + splitext(file_dest_single)[-1]
        file_dest_multi_pattern = (splitext(split(file_dest_single)[-1])[0] + '.\d{6}' + splitext(file_dest_single)[-1]).replace('.',  '\.')
        if exists(file_dest_single):
            # File already exists in the target directory, if stats are equal, file is same; if different, must be different file:
            return file_dest_single if stats_equal(filename, file_dest_single) else file_dest_multi
        else:
            return file_dest_multi if any([re.match(file_dest_multi_pattern, filename) for filename in os.listdir(path)]) else file_dest_single

def test_collect_activation(data_root=r"p:\data_test\source"):
    t = collect_source("P:\data\download", "ODIN4_ACTPROD", name='odin_activation', dest_name='activation',
                       data_root=data_root)
    t.copy()
def test_collect_ip(data_root=r"p:\data_test\source"):
    t = collect_source("P:\data\download", "IPLocationDB", name='ip_location', 
                       project='', client='common', data_root=data_root)
    t.copy()
def test_collect_tripolis(data_root=r"p:\data_test\source"):
    t = collect_source("P:\data\download", "exportContactsThatClicked", dest_name='clicked',
                       name='tripolis', data_root=data_root)
    t.copy()
    t = collect_source("P:\data\download", "exportContactsThatBounced", dest_name='bounced',
                       name='tripolis', data_root=data_root)
    t.copy()
    t = collect_source("P:\data\download", "exportContactsThatDidNotReceiveMail", dest_name='received_no_mail',
                       name='tripolis', data_root=data_root)
    t.copy()
    t = collect_source("P:\data\download", "exportContactsThatDidReceiveMail", dest_name='received_mail',
                       name='tripolis', data_root=data_root)
    t.copy()
    t = collect_source("P:\data\download", "exportContactsThatOpened", dest_name='opened',
                       name='tripolis', data_root=data_root)
    t.copy()
def test_collect_cendris(data_root=r"p:\data_test\source"):
    cendris = collect_source("P:\data\download", "ff_ids_levering.*\.txt", dest_name='cendris.csv',
                             name='cendris', extract_path='extract', 
                             data_root=data_root)
    cendris.copy()
def test_collect_qb(data_root=r"p:\data_test\source"):
    qb_ivp = collect_source("P:\data\download", "querybuilder_\d+.csv", 'odin_ivp.csv', ['FSECURE_KEY'], 
                            'odin_link', data_root=data_root)
    qb_bol = collect_source("P:\data\download", "querybuilder_\d+.csv", 'odin_bol.csv', ['BOL LICENTIE'], 
                            'odin_link', data_root=data_root)
    print qb_ivp._filenames
    print qb_bol._filenames
    qb_ivp.copy()
    qb_bol.copy()
def test_collect_odin(data_root=r"p:\data_test\source"):
    odin_z = collect_source("P:\data\download", "odin_dump\-.*\.zip", name='odin', extract_password='Z7pAjqQ6JHMzlA497y35gg==', 
                            extract_path='extract', data_root=data_root)
    #odin_z.copy(['.*2013\-10\-.*'])
    odin_z.copy()
    return
    odin = collect_source(r"P:\data\source\kpn\swol_marketing\odin", 'extract/odin_dump\.csv', 'odin.csv', name='odin')
    print odin._filenames
    odin.copy()

if __name__ == '__main__':
    #test_collect_activation()
    #test_collect_ip()
    #test_collect_tripolis()
    #test_collect_cendris()
    test_collect_odin(r"P:\data\source")
    test_collect_qb(r"P:\data\source")
