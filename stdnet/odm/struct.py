from uuid import uuid4
from collections import namedtuple

from stdnet.utils import iteritems, itervalues, missing_intervals, encoders,\
                            BytesIO, iterpair
from stdnet.lib import zset, skiplist
from stdnet import SessionNotAvailable

from .base import ModelBase
from .session import commit_when_no_transaction, withsession


__all__ = ['Structure',
           'Sequence',
           'OrderedMixin',
           'KeyValueMixin',
           'String',
           'List',
           'Set',
           'Zset',
           'HashTable',
           'TS',
           'NumberArray',
           # Mixins
           'OrderedMixin',
           'PairMixin',
           'KeyValueMixin']


################################################################################
##    CACHE CLASSES
################################################################################

class stringcache(object):
    
    def __init__(self):
        self.clear()
    
    def getvalue(self):
        return self.data.getvalue()
    
    def push_back(self, v):
        self.data.write(v)
        
    def clear(self):
        self.data = BytesIO()
    
    
class listcache(object):
    
    def __init__(self):
        self.clear()
        
    def push_front(self, value):
        self.front.append(value)
        
    def push_back(self, value):
        self.back.append(value)
        
    def clear(self):
        self.cache = []
        self.back = []
        self.front = []
        
    def set_cache(self, data):
        self.cache.extend(data)
        
    
class setcache(object):
    
    def __init__(self):
        self.cache = set()
        self.toadd = set()
        self.toremove = set()

    def __contains__(self, v):
        if v not in self.toremove:
            return v in self.cache or v in self.toadd
        
    def update(self, values):
        self.toadd.update(values)
        self.toremove.difference_update(values)
        
    def remove(self, values, add_to_remove = True):
        self.toadd.difference_update(values)
        if add_to_remove:
            self.toremove.update(values)
        else:
            self.toremove.difference_update(values)
        
    def clear(self):
        self.cache.clear()
        self.toadd.clear()
        self.toremove.clear()
        
    def set_cache(self, data):
        self.cache.update(data)


class zsetcache(setcache):
    
    def __init__(self):
        self.cache = zset()
        self.toadd = zset()
        self.toremove = set()
        
    
class hashcache(zsetcache):
    
    def __init__(self):
        self.cache = {}
        self.toadd = {}
        self.toremove = set()
        
    def remove(self, keys, add_to_remove = True):
        d = lambda x : self.toadd.pop(x,None)
        for key in keys:
            d(key)
        if add_to_remove:
            self.toremove.update(keys)
        else:
            self.toremove.difference_update(keys)


class tscache(hashcache):
    
    def __init__(self):
        self.cache = skiplist()
        self.toadd = skiplist()
        self.toremove = set()


################################################################################
##    STRUCTURE CLASSES
################################################################################

class Structure(ModelBase):
    '''A :class:`Model` which is used a base class for remote data-structures.
Remote structures are the
backend of :ref:`structured fields <model-field-structure>` but they
can also be used as stand alone objects. For example::

    import stdnet
    db = stdnet.getdb(...)
    mylist = db.list('bla')

.. attribute:: instance

    An optional :class:`StdModel` instance to which the structure belongs
    to via a :ref:`structured field <model-field-structure>`. This attribute
    is initialised by the :mod:`odm`.
    
    Default ``None``.
    
.. attribute:: timeout

    Expiry timeout. If different from zero it represents the number of seconds
    after which the structure is deleted from the data server.
    
    Default ``0``.

.. attribute:: pickler

    Class used for serialize and unserialize values.
    If ``None`` the :attr:`backend` pickler will be used.
    
    Default ``None``.
    '''
    _model_type = 'structure'
    pickler = None
    value_pickler = None
    def __init__(self, instance=None, timeout=0, value_pickler=None, **kwargs):
        self.instance = instance
        self.value_pickler = value_pickler or self.value_pickler or\
                                encoders.NumericDefault()
        self.timeout = timeout
        self.setup(**kwargs)
        if self.instance is not None:
            if not self.id:
                raise ValueError('Structure has instance but not id')
        elif not self.id:
            self.id = self.makeid()
        self._dbdata['id'] = self.id
        
    def obtain_session(self):
        if self.session is not None:
            return self.session.session()
        elif self.instance is not None:
            return self.instance.obtain_session()
        
    def makeid(self):
        return str(uuid4())[:8]
        
    def setup(self, **kwargs):
        pass
    
    @property
    def cache(self):
        if 'cache' not in self._dbdata:
            self._dbdata['cache'] = self.cache_class()
        return self._dbdata['cache']
        
    def __repr__(self):
        return '%s(%s) %s' % (self.__class__.__name__,self.id,self.cache)
        
    def __str__(self):
        return self.__repr__()
    
    def __iter__(self):
        cache = self.cache.cache
        if cache:
            return iter(cache)
        else:
            return iter(self._iter())
        
    @withsession
    def size(self):
        '''Number of elements in structure.'''
        return self.backend_structure().size()
    
    @withsession
    def __contains__(self, value):
        return self.pickler.dumps(value) in self.session.backend.structure(self)
    
    def __len__(self):
        return self.size()
    
    def _iter(self):
        loads = self.value_pickler.loads
        for v in self.session.structure(self):
            yield loads(v)
            
    def set_cache(self, data):
        '''Set the cache for the :class:`Structure`.
Do not override this function. Use :meth:`load_data` method instead.'''
        self.cache.clear()
        self.cache.set_cache(self.load_data(data))
        
    def load_data(self, data):
        '''Load data from the :class:`stdnet.BackendDataServer`.'''
        loads = self.value_pickler.loads
        return (loads(v) for v in data)
    
    def dbid(self):
        return self.backend_structure().id
    
    ############################################################################
    ## INTERNALS
    @withsession
    def backend_structure(self, client=None):
        return self.session.backend.structure(self, client)


################################################################################
##    Mixins Structures
################################################################################
class PairMixin(object):
    '''A mixin for handling structures with which holds pairs.'''
    pickler = encoders.NoEncoder()
    
    def setup(self, pickler = None, **kwargs):
        self.pickler = pickler or self.pickler
    
    def __setitem__(self, key, value):
        self.add(key, value)
    
    @withsession
    def items(self):
        data = self.session.structure(self).items()
        return self.load_data(data)
        
    def pair(self, pair):
        '''Add a *pair* to the structure.'''
        if len(pair) == 1:
            # if only one value is passed, the value must implement a
            # score function which retrieve the first value of the pair
            # (score in zset, timevalue in timeseries, field value in hashtable)
            return (pair[0].score(),pair[0])
        elif len(pair) != 2:
            raise TypeError('add expected 2 arguments, got {0}'\
                            .format(len(pair)))
        else:
            return pair
        
    def add(self, *pair):
        self.update((pair,))
        
    @commit_when_no_transaction
    def update(self, mapping):
        '''Add *mapping* dictionary to hashtable.
Equivalent to python dictionary update method.

:parameter mapping: a dictionary of field values.'''
        self.cache.update(self.dump_data(mapping))

    def dump_data(self, mapping):
        tokey = self.pickler.dumps
        dumps = self.value_pickler.dumps
        if isinstance(mapping,dict):
            mapping = iteritems(mapping)
        p = self.pair
        for pair in mapping:
            if not isinstance(pair,tuple):
                pair = pair,
            k,v = p(pair)
            yield tokey(k),dumps(v)
    
    def load_data(self, mapping):
        loads = self.pickler.loads
        vloads = self.value_pickler.loads
        return ((loads(k),vloads(v)) for k,v in iterpair(mapping))
    
    def load_keys(self, iterable):
        loads = self.pickler.loads
        return (loads(k) for k in iterable)
    
    def load_values(self, iterable):
        vloads = self.value_pickler.loads
        return (vloads(v) for v in iterable)
    

class KeyValueMixin(PairMixin):
    '''A mixin for ordered and unordered key-valued pair containers.
A key-value pair container has the :meth:`values` and :meth:`items`
methods, while its iterator is over keys.'''
    def _iter(self):
        return self.keys()
            
    def __delitem__(self, key):
        '''Immediately remove an element. To remove with transactions use the
:meth:`remove` method`.'''
        self.pop(key)

    @withsession
    def __getitem__(self, key):
        dkey = self.pickler.dumps(key)
        res = self.session.backend.structure(self).get(dkey)
        return self.async_handle(res, self._load_get_data, key)
    
    def get(self, key, default=None):
        '''Retrieve a single element from the structure.
If the element is not available return the default value.

:parameter key: lookup field
:parameter default: default value when the field is not available'''
        dkey = self.pickler.dumps(key)
        res = self.session.backend.structure(self).get(dkey)
        return self.async_handle(res, self._load_get_data, key, default)
        
    def pop(self, key, *args):
        dkey = self.pickler.dumps(key)
        res = self.session.backend.structure(self).pop(dkey)
        if len(args) == 1:
            return self.async_handle(res, self._load_get_data, key, args[0])
        elif not args:
            return self.async_handle(res, self._load_get_data, key)
        else:
            raise TypeError('pop expected at most 2 arguments, got {0}'\
                            .format(len(args)+1))
        
    @commit_when_no_transaction
    def remove(self, *keys):
        '''Remove *keys* from the key-value container.'''
        dumps = self.pickler.dumps
        self.cache.remove([dumps(v) for v in keys])
        
    def keys(self):
        raise NotImplementedError()
    
    @withsession
    def values(self):
        vloads = self.value_pickler.loads
        for v in self.session.structure(self).values():
            yield vloads(v)
    
    def load_get_data(self, value):
        return self.value_pickler.loads(value)
    
    # INTERNALS
    def _load_get_data(self, value, key, *args):
        if value is None:
            if not args:
                raise KeyError(key)
            else:
                return args[0]
        return self.load_get_data(value)
    
    
class OrderedMixin(object):
    '''A mixin for a :class:`Structure` which maintains ordering with respect
a numeric value we call score.'''
    
    def front(self):
        '''Return the front pair of the structure'''
        v = tuple(self.irange(0, 0))
        if v:
            return v[0]
    
    def back(self):
        '''Return the back pair of the structure'''
        v = tuple(self.irange(-1, -1))
        if v:
            return v[0]
    
    def count(self, start, stop):
        '''Count the number of elements bewteen *start* and *stop*.'''
        s1 = self.pickler.dumps(start)
        s2 = self.pickler.dumps(stop)
        return self.backend_structure().count(s1, s2)
    
    def range(self, start, stop, callback=None, **kwargs):
        '''Return a range with scores between start and end.'''
        s1 = self.pickler.dumps(start)
        s2 = self.pickler.dumps(stop)
        res = self.backend_structure().range(s1, s2, **kwargs)
        return self.async_handle(res, callback or self.load_data)
    
    def irange(self, start=0, end=-1, callback=None, **kwargs):
        '''Return the range by rank between start and end.'''
        res = self.backend_structure().irange(start, end, **kwargs)
        return self.async_handle(res, callback or self.load_data)
        
    def pop_range(self, start, stop, callback=None, **kwargs):
        '''pop a range by score from the :class:`OrderedMixin`'''
        s1 = self.pickler.dumps(start)
        s2 = self.pickler.dumps(stop)
        res = self.backend_structure().pop_range(s1, s2, **kwargs)
        return self.async_handle(res, callback or self.load_data)

    def ipop_range(self, start=0, stop=-1, callback=None, **kwargs):
        '''pop a range from the :class:`OrderedMixin`'''
        res = self.backend_structure().ipop_range(start, stop, **kwargs)
        return self.async_handle(res, callback or self.load_data)
    

class Sequence(object):
    '''Mixin for a :class:`Structure` which implements a kind of sequence
container. The elements in a sequence container are ordered following a linear
sequence.'''
    cache_class = listcache
    
    @commit_when_no_transaction
    def push_back(self, value):
        '''Appends a copy of *value* at the end of the :class:`Sequence`.'''
        self.cache.push_back(self.value_pickler.dumps(value))
        return self
        
    def pop_back(self):
        '''Remove the last element from the :class:`Sequence`.'''
        value = self.session.structure(self).pop_back()
        return self.value_pickler.loads(value)
    
    def __getitem__(self, index):
        value = self.session.structure(self).get(index)
        return self.value_pickler.loads(value)
    
    def __setitem__(self, index, value):
        value = self.value_pickler.dumps(value)
        self.session.structure(self).set(index,value)
    
    
################################################################################
##    STRUCTURES
################################################################################

class Set(Structure):
    '''An unordered set :class:`Structure`. Equivalent to a python ``set``.'''
    cache_class = setcache
    
    @commit_when_no_transaction
    def add(self, value):
        '''Add *value* to the set'''
        return self.cache.update((self.value_pickler.dumps(value),))
    
    @commit_when_no_transaction
    def update(self, values):
        '''Add iterable *values* to the set'''
        d = self.value_pickler.dumps
        return self.cache.update(tuple((d(v) for v in values)))
        
    @commit_when_no_transaction
    def discard(self, value):
        '''Remove an element *value* from a set if it is a member.'''
        return self.cache.remove((self.value_pickler.dumps(value),))
    remove = discard
    
    @commit_when_no_transaction
    def difference_update(self, values):
        '''Remove an iterable of *values* from the set.'''
        d = self.value_pickler.dumps
        return self.cache.remove(tuple((d(v) for v in values)))
    
    
class List(Sequence, Structure):
    '''A doubly-linked list :class:`Structure`. It expands the
:class:`Sequence` mixin with functionalities to add and remove from
the front of the list in an efficient manner.'''
    def pop_front(self):
        '''Remove the first element from of the list.'''
        value = self.session.structure(self).pop_front()
        return self.value_pickler.loads(value)
    
    def block_pop_back(self, timeout = None):
        value = self.session.structure(self).block_pop_back(timeout)
        return self.value_pickler.loads(value)
    
    def block_pop_front(self, timeout = None, transaction = None):
        value = self.session.structure(self).block_pop_front(timeout)
        return self.value_pickler.loads(value)
    
    @commit_when_no_transaction
    def push_front(self, value):
        '''Appends a copy of *value* to the beginning of the list.'''
        self.cache.push_front(self.value_pickler.dumps(value))


class Zset(OrderedMixin, PairMixin, Set):
    '''An ordered version of :class:`Set`. It derives from
:class:`OrderedMixin` and :class:`PairMixin`.'''
    pickler = encoders.Double()
    cache_class = zsetcache
    
    def rank(self, value):
        '''The rank of a given *value*. This is the position of *value*
in the :class:`OrderedMixin` container.'''
        value = self.pickler.dumps(value)
        return self.backend_structure().rank(value)
    
    def _iter(self):
        # Override the KeyValueMixin so that it iterates over values rather
        # scores
        loads = self.value_pickler.loads
        for v in self.session.structure(self):
            yield loads(v)
                    
    
class HashTable(KeyValueMixin, Structure):
    '''A :class:`Structure` which is the networked equivalent to
a Python ``dict``. Derives from :class:`KeyValueMixin`.'''
    cache_class = hashcache
    
    def keys(self):
        loads = self.pickler.loads
        for k in self.session.structure(self):
            yield loads(k)
        
    def addnx(self, field, value, transaction = None):
        '''Set the value of a hash field only if the field
does not exist.'''
        return self._addnx(self.cursor(transaction),
                           self.pickler.dumps(key),
                           self.pickler_value.dumps(value))

    
class TS(OrderedMixin, KeyValueMixin, Structure):
    '''A timeseries is a :class:`Structure` which derives from 
:class:`OrderedMixin` and :class:`KeyValueMixin`.
It represents an ordered associative container where keys are timestamps
and values are objects.'''
    pickler = encoders.DateTimeConverter()
    value_pickler = encoders.Json()
    cache_class = tscache
    
    def items(self):
        return self.irange()
    
    def keys(self):
        return self.itimes()
    
    def rank(self, dte):
        '''The rank of a given *dte* in the timeseries'''
        timestamp = self.pickler.dumps(dte)
        return self.backend_structure().rank(timestamp)
    
    def ipop(self, index):
        '''Pop a value at *index* from the :class:`TS`. Return ``None`` if
index is not out of bound.'''
        res = self.session.backend.structure(self).ipop(index)
        return self.async_handle(res, self._load_get_data, index, None)
    
    def times(self, start, stop, callback=None, **kwargs):
        '''The times between times *start* and *stop*.'''
        s1 = self.pickler.dumps(start)
        s2 = self.pickler.dumps(stop)
        res = self.backend_structure().times(s1, s2, **kwargs)
        return self.async_handle(res, callback or self.load_keys)

    def itimes(self, start=0, stop=-1, callback=None, **kwargs):
        '''The times between rank *start* and *stop*.'''
        res = self.backend_structure().itimes(start, stop, **kwargs)
        return self.async_handle(res, callback or self.load_keys)
    
    
class String(Sequence, Structure):
    '''A String :class:`Sequence` of bytes.
'''
    cache_class = stringcache
    value_pickler = encoders.Bytes()

    def incr(self, v = 1):
        return self.backend_structure().incr(v)
    
    
class Array(Sequence, Structure):
    
    def resize(self, size):
        return self.backend_structure().resize(size)
    
    def capacity(self):
        return self.backend_structure().capacity()
    
    
class NumberArray(Array):
    '''A compact :class:`Array` containing numbers.'''
    value_pickler = encoders.CompactDouble()
    