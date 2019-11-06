from os import getcwd, walk, makedirs
from os.path import defpath, expanduser, join, split, splitext
import os, re, datetime, yaml, pprint, pyDes, base64
KEY = 'import xml, sys, shutils'  # 24 characters, not strong, but code is hidden enough...

def encrypt(value):
    print base64.b64encode(pyDes.triple_des(KEY, padmode=pyDes.PAD_PKCS5).encrypt(value))

def decrypt(loader, node):
    return pyDes.triple_des(KEY, padmode=pyDes.PAD_PKCS5).decrypt(base64.b64decode(node.value))

yaml.add_constructor('tag:yaml.org,2002:decrypt', decrypt, Loader=yaml.loader.SafeLoader)

class ConfigKey(dict):
    def __init__(self, data, key=None, parent=None):
        # Watch out: ALL instance variables MUST BE excepted in __setattr__!
        self._data = data
        self._key = key
        self._parent = parent
        self._filename = None
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError("ConfigKey: '{}' has no attribute '{}'".format(self.path(), key))
    def __getitem__(self, key):
        data = self._data[key]
        if isinstance(data, dict):
            return ConfigKey(data, key, self)
        if isinstance(data, list):
            return [ConfigKey(e, self) if isinstance(e, dict) else e for e in data]
        return data
    def __setitem__(self, key, value):
        self._data[key] = value
    def __setattr__(self, key, value):
        if key in ('_data', '_key', '_parent', '_filename'):
            dict.__setattr__(self, key, value)
        else:
            self._data[key] = value
    def __iter__(self):
        for k in self._data:
            yield k
    def clear(self):
        self._data.clear()
    def items(self):
        return [(k,v) for k,v in self.iteritems()]
    def iteritems(self):
        for k in self:
            yield(k, self[k])
    iterkeys = __iter__
    def itervalues(self):
        for key in self:
            yield self[key]
    def keys(self):
        return self._data.keys()
    def pop(self, key, default=None):
        return self._data.pop(key,default)
    def popitem(self):
        if not self:
            raise KeyError
        k = self.keys()[0]
        v = self[k]
        del self[k]
        return k, v
    def setdefault(self, k, d=None):
        if k not in self:
            self[k] = d
        return self[k]
    def update(self, other):
        for k ,v in other.iteritems():
            self[k] = v
    def copy(self):
        return self.items()
    def __len__(self):
        return len(self._data)
    def values(self):
        return [self[k] for k in self]
    def viewitems(self):
        return self._data.viewitems()
    def viewkeys(selfself):
        return selfself._data.viewkeys()
    def viewvalues(self):
        return self._data.viewvalues()
    def __delattr__(self, key):
        del self._data[key]
    def __contains__(self, key):
        return key in self._data
    def get(self, name, default=None):
        return getattr(self, name) if name in self._data else default
    def path(self):
        return '.'.join(self.path_list())
    def path_list(self):
        if self._parent:
            return self._parent.path_list() + [self._key]
        return []
    def __repr__(self):
        return yaml.dump(self._data)

class Config(ConfigKey):
    """Config-object with support for nested data. For storage, the yaml-format is used.
    Include files with the include-keyword. Data in the current file always overrules that in the 
    included file. Include files can not contain referenced values, but should be no limitation
    by using relative references.
    References may be made by enclosing the reference in pointed braces,
    rules for scoping follow that of regular lexical scoping.
    If a reference is not found, the sharp braces remain. A value referencing that value is also unchanged.
    """
    def __init__(self, filename='biz.yml'):
        # Now find the file in one of the following locations:
        super(Config, self).__init__({})
        def get_includes(data, path):
            "Include the data in the filename in the supplied data (in-place changing)."
            def merge(source, add):
                if not isinstance(data, dict) or not isinstance(add, dict):
                    return       # Nothing to merge
                for key in add:
                    if key in source:
                        merge(source[key], add[key])
                    else:
                        source[key] = add[key]
            if 'include' in data:
                include = data['include']
                if not isinstance(include, list):
                    include = [include]
                for filename in include:
                    # Join the filename and the path, to process relative paths in the filename.
                    full_filename = join(path, filename)
                    add = yaml.safe_load(open(full_filename))
                    get_includes(add, split(full_filename)[0])
                    merge(data, add)
        pathlist = [getcwd(), expanduser('~')] + defpath.split(';')
        found = False
        for path in pathlist:
            if os.path.exists(join(path, filename)):
                self._filename = join(path, filename)
                found = True
                self._data = yaml.safe_load(open(self._filename))
                get_includes(self._data, split(self._filename)[0])
                break
        if not found:
            self.filename = join(path[0], filename)
            self._data = {}
        self._data = self.resolve_references(self._data)
    def resolve_references(self, data):
        def resolve(current, ancestors=[]):
            def repl(match):
                target = match.group(0)
                keys = target[1:-1].split('.')  # Strip sharp braces, split in components, separated by dots
                for a in ancestors:
                    tree = a
                    for key in keys:
                        if not isinstance(tree, dict):
                            tree = None
                            break
                        if key in tree:
                            tree = tree[key]
                        else:
                            tree = None
                    if isinstance(tree, basestring) and not re.match('.*<.*>.*', tree):
                        self._changes += 1
                        return tree
                return target
            if isinstance(current, basestring):
                return re.sub('<.*?>', repl, current)
            if isinstance(current, list):
                return [resolve(i, ancestors) for i in current]
            if isinstance(current, dict):
                for k, v in current.items():
                    current[k] = resolve(v, [current] + ancestors)
            return current
        self._changes = 0
        while True:
            # Iterate through the reference-resolver to enable recursive references to clear up:
            changes_old = self._changes
            data = resolve(data)
            if self._changes == changes_old:
                return data
    
    
def test():
    c = Config()
    print c

if __name__ == '__main__':
    test()