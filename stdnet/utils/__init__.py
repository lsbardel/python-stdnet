import os
import sys
from itertools import chain
from uuid import uuid4

from stdnet.lib.py2py3 import *

if ispy3k:
    import pickle
    unichr = chr
else:
    import cPickle as pickle
    unichr = unichr 
    
from .jsontools import *
from .populate import populate
from .dates import *


def gen_unique_id():
    return str(uuid4())[:8]


def _flat2d_gen(iterable):
    for v in iterable:
        yield v[0]
        yield v[1]
        
        
def flat2d(iterable):
    if hasattr(iterable,'__len__'):
        return chain(*iterable)
    else:
        return _flat2d_gen(iterable)

    
def _flatzsetdict(kwargs):
    for k,v in iteritems(kwargs):
        yield v
        yield k


def flatzset(iterable = None, kwargs = None):
    if iterable:
        c = flat2d(iterable)
        if kwargs:
            c = chain(c,_flatzsetdict(kwargs))
    elif kwargs:
        c = _flatzsetdict(kwargs)
    return tuple(c)


def uplevel(path,lev=1):
    if lev:
        return uplevel(os.path.split(path)[0],lev-1)
    else:
        return path


class PPath(object):
    '''Utility class for adding directories to the python path'''    
    def __init__(self, local_path):
        local_path = os.path.abspath(local_path)
        if os.path.isfile(local_path):
            self.local_path = os.path.split(local_path)[0]
        elif os.path.isdir(local_path):
            self.local_path = local_path
        else:
            raise ValueError('{0} not a valid directory'.format(local_path))
        
    def __repr__(self):
        return self.local_path
    __str__ = __repr__
    
    def join(self, path):
        return os.path.join(self.local_path,path)
        
    def add(self, module = None, up = 0, down = None, front = False):
        '''Add a directory to the python path.
        
:parameter module: Optional module name to try to import once we have found
    the directory
:parameter up: number of level to go up the directory three from
    :attr:`local_path`.
:parameter down: Optional tuple of directory names to travel down once we have
    gone *up* levels.
:parameter front: Boolean indicating if we want to insert the new path at the
    front of ``sys.path`` using ``sys.path.insert(0,path)``.'''
        if module:
            try:
                __import__(module)
                return module
            except ImportError:
                pass
            
        dir = uplevel(self.local_path,up)
        if down:
            dir = os.path.join(dir, *down)
        added = False
        if os.path.isdir(dir):
            if dir not in sys.path:
                if front:
                    sys.path.insert(0,dir)
                else:
                    sys.path.append(dir)
                added = True
            else:
                raise ValueError('Directory {0} not available'.format(dir))
        if module:
            try:
                __import__(module)
                return module
            except ImportError:
                pass
        return added
