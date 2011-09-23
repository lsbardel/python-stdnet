from itertools import chain

from stdnet.lib.py2py3.py2py3 import *

if ispy3k:
    import pickle
    from io import BytesIO
    unichr = chr
else:
    import cPickle as pickle
    from cStringIO import StringIO as BytesIO
    unichr = unichr 
    
from .jsontools import *
from .populate import populate
from .dates import *


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
