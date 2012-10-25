import os
import sys
from itertools import chain
from collections import Mapping
from uuid import uuid4

from .py2py3 import *

if ispy3k:  # pragma: no cover
    import pickle
    unichr = chr
else:   # pragma: no cover
    import cPickle as pickle
    unichr = unichr 
    
from .jsontools import *
from .populate import populate
from .dates import *
from .path import *


def gen_unique_id(short = True):
    id = str(uuid4())
    if short:
        id = id[:8]
    return id

def iterpair(iterable):
    if isinstance(iterable, Mapping):
        return iteritems(iterable)
    else:
        return iterable
    
def int_or_float(v):
    v = float(v)
    i = int(v)
    return i if v==i else v

def grouper(n, iterable, padvalue=None):
    "grouper(3, 'abcdefg', 'x') --> ('a','b','c'), ('d','e','f'), ('g','x','x')"
    return zip_longest(*[iter(iterable)]*n, fillvalue=padvalue)

def _format_int(val):
    positive = val >= 0
    sval = ''.join(reversed(','.join((''.join(g) for g in\
                               grouper(3,reversed(str(abs(val))),'')))))
    return sval if positive else '-'+sval
    
def format_int(val):
    try: # for python 2.7 and up
        return '{:,}'.format(val)
    except ValueError:  # pragma nocover
        _format_int(val)

def flat_mapping(mapping):
    if isinstance(mapping,dict):
        mapping = iteritems(mapping)
    items = []
    extend = items.extend
    for pair in mapping:
        extend(pair)
    return items


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

def unique_tuple(*iterables):
    vals = []
    for v in chain(*[it for it in iterables if it]):
        if v not in vals:
            vals.append(v)
    return tuple(vals)

memory_symbols = ('K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')
memory_size = dict(((s,1 << (i+1)*10) for i,s in enumerate(memory_symbols)))

def convert_bytes(b):
    '''Convert a number of bytes into a human readable memory usage'''
    if b is None:
        return '#NA'
    for s in reversed(memory_symbols):
        if b >= memory_size[s]:
            value = float(b) / memory_size[s]
            return '%.1f%sB' % (value, s)
    return "%sB" % b
