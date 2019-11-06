# basetypes.py - Find and compile all available converters for all available types.
import sys, importlib
types = importlib.import_module('types')

basetypes = {}

def basetype(series):
    """Return the basetype and optional the parameters for extracting the values from the string.
    Possible return types are:
    str
    int
    float
    datetime
    bool"""
    if not series.dtype == types.ObjectType:
        # Type is specific already; return it without further inspection.
        return series.dtype
    # Now type is Object (string) See if any more specific type can be deduced:
    
class Basetype(object):
    "A container for a basetype-module with converters and matches."
    def __init__(self, name, root_module):
        self.name = name
        self.corpus = {}
        self.categorizers = {k:v for k,v in sys.modules[__name__].__dict__.items() if is_cat(k, v)}
        # import the specified module; give it a unique name:
        modulename = root_module.rstrip('.') + '.' + name.strip('.')
        try:
            importlib.import_module(modulename)
            # Get the module:
            module = sys.modules[modulename]
            for k, v in module.__dict__.items():
                if not k.startswith('__') and not type(v) is type(module):
                    # Non-module-type object; function, constant etc:
                    if is_cat(k, v):
                        self.categorizers[k] = v
                    else:
                        setattr(self, k, v)
        

def dotted(dirpath, relative_to="", root_module=""):
    """Return the dotted representation of the dirpath, with directory separators replaced with dots.
    If a relative root is given, remove it from the start of the dirpath."""
    def path_components(dirpath):
        d, p = split(dirpath)
        if d == dirpath:
            return [d]
        else:
            return path_components(d) + [p]
    dirs = path_components(dirpath)
    relatives = path_components(relative_to)
    while relatives and dirs and dirs[0] == relatives[0]:
        dirs, relatives = dirs[1:], relatives[1:]
    if root_module:
        roots = root_module.split('.')
        while roots and dirs and dirs[0] == roots[0]:
            dirs, roots = dirs[1:], roots[1:]
    return '.'.join(dirs)

def basetype_module(path, sys_root):
    """Return the modulename and module  belonging to the specified path if it is a valid type-module. 
    If not, return (None, None)."""
    no_mod = None, None
    if splitext(path)[-1].lower() != '.py' or split(path)[-1].startswith('_'):
        return no_mod
    mod_name = dotted(splitext(path)[0], p_sys)
    shortname = dotted(splitext(path)[0], p_sys, self.root_module)
    mod = importlib.import_module(mod_name)
    for fname in 'convert type'.split():
        if hasattr(mod, fname) and callable(getattr(mod, fname)):
            return shortname, mod
    return no_mod

def load_basetypes(root_module='biz.basetypes'):
    global basetypes
    mod = importlib.import_module(root_module)
    for p_mod in mod.__path__:
        # Get all modules, starting at the root-module:
        for p_sys in sys.path:
            if dotted(p_mod, p_sys) == root_module:
                break
        for dirpath, dirnames, filenames in os.walk(p_mod):
            for filename in filenames:
                mod = basetype_module(join(dirpath, filename), p_sys)
                if mod_name and mod_name not in self.types:
                    self.types.setdefault(mod_name, Type(mod_name, self.root_module))
load_basetypes()