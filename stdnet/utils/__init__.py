from inspect import istraceback
from itertools import chain
from collections import Mapping
from uuid import uuid4

from .py2py3 import *

if ispy3k:  # pragma: no cover
    import pickle
    unichr = chr

    def raise_error_trace(err, traceback):
        if istraceback(traceback):
            raise err.with_traceback(traceback)
        else:
            raise err

else:   # pragma: no cover
    import cPickle as pickle
    unichr = unichr
    from .fallbacks.py2 import raise_error_trace

from .jsontools import *  # noqa
from .populate import populate  # noqa
from .dates import *  # noqa


def gen_unique_id(short=True):
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
    return i if v == i else v


def grouper(n, iterable, padvalue=None):
    '''grouper(3, 'abcdefg', 'x') --> ('a','b','c'), ('d','e','f'),
    ('g','x','x')'''
    return zip_longest(*[iter(iterable)] * n, fillvalue=padvalue)


def _format_int(val):
    positive = val >= 0
    sval = ''.join(reversed(','.join((
        ''.join(g) for g in grouper(3, reversed(str(abs(val))), '')))))
    return sval if positive else '-' + sval


def format_int(val):
    try:  # for python 2.7 and up
        return '{:,}'.format(val)
    except ValueError:  # pragma nocover
        _format_int(val)


def flat_mapping(mapping):
    items = []
    extend = items.extend
    for pair in iterpair(mapping):
        extend(pair)
    return items


def _flat2d_gen(iterable):
    for v in iterable:
        yield v[0]
        yield v[1]


def flat2d(iterable):
    if hasattr(iterable, '__len__'):
        return chain(*iterable)
    else:
        return _flat2d_gen(iterable)


def _flatzsetdict(kwargs):
    for k, v in iteritems(kwargs):
        yield v
        yield k


def flatzset(iterable=None, kwargs=None):
    if iterable:
        c = flat2d(iterable)
        if kwargs:
            c = chain(c, _flatzsetdict(kwargs))
    elif kwargs:
        c = _flatzsetdict(kwargs)
    return tuple(c)


def unique_tuple(*iterables):
    vals = []
    for v in chain(*[it for it in iterables if it]):
        if v not in vals:
            vals.append(v)
    return tuple(vals)
