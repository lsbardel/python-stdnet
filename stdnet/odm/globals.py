import hashlib
from collections import namedtuple

from stdnet.utils import to_bytes, JSPLITTER

__all__ = ['get_model_from_hash',
           'get_hash_from_model',
           'hashmodel',
           'JSPLITTER']

# Information about a lookup in a query
lookup_value = namedtuple('lookup_value', 'lookup value')

# Utilities for sorting and range lookups
orderinginfo = namedtuple('orderinginfo', 'name field desc model nested auto')

# attribute name, field, model where to do lookup, nested lookup_info
range_lookup_info = namedtuple('range_lookup_info', 'name field model nested')


class ModelDict(dict):

    def from_hash(self, hash):
        return self.get(hash)

    def to_hash(self, model):
        return model._meta.hash

_model_dict = ModelDict()


def get_model_from_hash(hash):
    return _model_dict.from_hash(hash)


def get_hash_from_model(model):
    return _model_dict.to_hash(model)


def hashmodel(model, library=None):
    '''Calculate the Hash id of metaclass ``meta``'''
    library = library or 'python-stdnet'
    meta = model._meta
    sha = hashlib.sha1(to_bytes('{0}({1})'.format(library, meta)))
    hash = sha.hexdigest()[:8]
    meta.hash = hash
    if hash in _model_dict:
        raise KeyError('Model "{0}" already in hash table.\
 Rename your model or the module containing the model.'.format(meta))
    _model_dict[hash] = model


def _make_id(target):
    if hasattr(target, '__func__'):
        return (id(target.__self__), id(target.__func__))
    return id(target)


class Event:

    def __init__(self):
        self.callbacks = []

    def bind(self, callback, sender=None):
        '''Bind a ``callback`` for a given ``sender``.'''
        key = (_make_id(callback), _make_id(sender))
        self.callbacks.append((key, callback))

    def fire(self, sender=None, **params):
        '''Fire callbacks from a ``sender``.'''
        keys = (_make_id(None), _make_id(sender))
        results = []
        for (_, key), callback in self.callbacks:
            if key in keys:
                results.append(callback(self, sender, **params))
        return results

    def unbind(self, callback, sender=None):
        key = (_make_id(callback), _make_id(sender))
        for index, key_cbk in enumerate(self.callbacks):
            if key == key_cbk[0]:
                del self.callbacks[index]
                break
