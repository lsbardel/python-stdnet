from uuid import uuid4

from stdnet.utils import iteritems, encoders, BytesIO, iterpair, ispy3k
from stdnet.utils.zset import zset
from stdnet.utils.skiplist import skiplist
from stdnet.utils.async import on_result, async, is_async

from .base import ModelBase


__all__ = ['Structure',
           'StructureCache',
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
           'KeyValueMixin',
           'commit_when_no_transaction']


def commit_when_no_transaction(f):
    '''Decorator for committing changes when the instance session is
not in a transaction.'''
    def _(self, *args, **kwargs):
        r = f(self, *args, **kwargs)
        return self.session.add(self) if self.session is not None else r
    _.__name__ = f.__name__
    _.__doc__ = f.__doc__
    return _

################################################################################
##    CACHE CLASSES FOR STRUCTURES
################################################################################
class StructureCache(object):
    '''Interface for all :attr:`Structure.cache` classes.'''
    def __init__(self):
        self.clear()
    
    def __str__(self):
        if self.cache is None:
            return ''
        else:
            return str(self.cache)
        
    def clear(self):
        '''Clear the cache for data'''
        self.cache = None
        
    def items(self):
        return self.cache
        
    def set_cache(self, data):
        raise NotImplementedError()
    
    
class stringcache(StructureCache):
    
    def getvalue(self):
        return self.data.getvalue()
    
    def push_back(self, v):
        self.data.write(v)
        
    def clear(self):
        self.cache = None
        self.data = BytesIO()
    
    
class listcache(StructureCache):
        
    def push_front(self, value):
        self.front.append(value)
        
    def push_back(self, value):
        self.back.append(value)
        
    def clear(self):
        self.cache = None
        self.back = []
        self.front = []
        
    def set_cache(self, data):
        if self.cache is None:
            self.cache = []
        self.cache.extend(data)
        
    
class setcache(StructureCache):

    def __contains__(self, v):
        if v not in self.toremove:
            return v in self.cache or v in self.toadd
        
    def update(self, values):
        self.toadd.update(values)
        self.toremove.difference_update(self.toadd)
        
    def remove(self, values, add_to_remove = True):
        self.toadd.difference_update(values)
        if add_to_remove:
            self.toremove.update(values)
        else:
            self.toremove.difference_update(values)
        
    def clear(self):
        self.cache = None
        self.toadd = set()
        self.toremove = set()
        
    def set_cache(self, data):
        if self.cache is None:
            self.cache = set()
        self.cache.update(data)


class zsetcache(setcache):
    
    def clear(self):
        self.cache = None
        self.toadd = zset()
        self.toremove = set()
        
    def set_cache(self, data):
        if self.cache is None:
            self.cache = zset()
        self.cache.update(data)
        
    
class hashcache(zsetcache):
    
    def clear(self):
        self.cache = None
        self.toadd = {}
        self.toremove = set()
    
    def set_cache(self, data):
        if self.cache is None:
            self.cache = {}
        self.cache.update(data)
        
    def items(self):
        return self.cache.items()
        
    def remove(self, keys, add_to_remove = True):
        d = lambda x : self.toadd.pop(x,None)
        for key in keys:
            d(key)
        if add_to_remove:
            self.toremove.update(keys)
        else:
            self.toremove.difference_update(keys)


class tscache(hashcache):
    
    def clear(self):
        self.cache = None
        self.toadd = skiplist()
        self.toremove = set()

    def set_cache(self, data):
        if self.cache is None:
            self.cache = skiplist()
        self.cache.update(data)


################################################################################
##    STRUCTURE CLASSES
################################################################################
class Structure(ModelBase):
    '''A :class:`Model` which is used a base class for remote data-structures.
Remote structures are the backend of
:ref:`structured fields <model-field-structure>` but they
can also be used as stand alone objects. For example::

    import stdnet
    db = stdnet.getdb(...)
    mylist = db.list('bla')

.. attribute:: cache

    A python :class:`StructureCache` for this :class:`Structure`.
    The :attr:`cache` is used when adding or removing data via
    :class:`Transaction` as well as
    for storing the results obtained form a call to :meth:`items`.

.. attribute:: value_pickler

    Class used for serialize and unserialize values.
    If ``None`` the :attr:`stdnet.utils.encoders.NumericDefault`
    will be used.
    
    Default ``None``.
    
.. attribute:: field

    The :class:`StructureField` which this owns this :class:`Structure`.
    Default ``None``.

'''
    _model_type = 'structure'
    abstract = True
    pickler = None
    value_pickler = None
    def __init__(self, value_pickler=None, name='', field=False, session=None,
                 pkvalue=None, **kwargs):
        self._field = field
        self._pkvalue = pkvalue
        self.name = name
        self.value_pickler = value_pickler or self.value_pickler or\
                                encoders.NumericDefault()
        self.setup(**kwargs)
        self.session = session
        if not self.id and not self._field:
            self.id = self.makeid()
            self._dbdata['id'] = self.id
        
    def makeid(self):
        return str(uuid4())[:8]
        
    def setup(self, **kwargs):
        pass
    
    @property
    def field(self):
        return self._field
    
    @property
    def model(self):
        '''The :class:`StdModel` which contains the :attr:`field` of this
:class:`Structure`. Only available if :attr:`field` is defined.'''
        if self._field:
            return self._field.model
        
    @property
    def cache(self):
        if 'cache' not in self._dbdata:
            self._dbdata['cache'] = self.cache_class()
        return self._dbdata['cache']
        
    def __repr__(self):
        return '%s %s' % (self.__class__.__name__, self.cache)
        
    def __str__(self):
        return self.__repr__()
    
    def __iter__(self):
        # Iterate through the structure
        res = self.items()
        if is_async(res):
            raise RuntimeError('Cannot iterate on asynchronous result.')
        else:
            return iter(res)
        
    def size(self):
        '''Number of elements in the :class:`Structure`.'''
        if self.cache.cache is None:
            return self.read_backend_structure().size()
        else:
            return len(self.cache.cache)
    
    def __contains__(self, value):
        return self.pickler.dumps(value) in self.read_backend_structure()
    
    def __len__(self):
        return self.size()
    
    def items(self):
        '''All items of this :class:`Structure`. Implemented by subclasses.'''
        raise NotImplementedError
    
    def set_cache(self, data):
        '''Set the cache for the :class:`Structure`.
Do not override this function. Use :meth:`load_data` method instead.'''
        self.cache.clear()
        self.cache.set_cache(self.load_data(data))
        
    def load_data(self, data):
        '''Load ``data`` from the :class:`stdnet.BackendDataServer`.'''
        return self.value_pickler.load_iterable(data, self.session)
    
    def backend_structure(self, client=None):
        '''Returns a valid :class:`stdnet.BackendStructure` for this
:class:`Structure`.'''
        if self._field:
            backend = self.session.model(self._field.model).backend
        else:
            backend = self.session.model(self).backend
        return backend.structure(self, client)
    
    def read_backend_structure(self, client=None):
        if self._field:
            backend = self.session.model(self._field.model).read_backend
        else:
            backend = self.session.model(self).read_backend
        return backend.structure(self, client)


################################################################################
##    Mixins Structures
################################################################################
class PairMixin(object):
    '''A mixin for structures with which holds pairs. It is the parent class
of :class:`KeyValueMixin` and it is used as base class for the ordered set
structure :class:`Zset`.

.. attribute:: pickler

    An :ref:`encoder <encoders>` for the additional value in the pair.
    The additional value is a field key for :class:`Hashtable`,
    a numeric score for :class:`Zset` and a tim value for :class:`TS`. 
    
'''
    pickler = encoders.NoEncoder()
    
    def setup(self, pickler=None, **kwargs):
        self.pickler = pickler or self.pickler
    
    def __setitem__(self, key, value):
        self.add(key, value)
    
    @async()
    def items(self):
        '''Iterator over items (pairs) of :class:`PairMixin`.'''
        if self.cache.cache is None:
            data = yield self.read_backend_structure().items()
            data = yield self.load_data(data)
            self.cache.set_cache(data)
        yield self.cache.items()
    
    @async()
    def values(self):
        '''Iteratir over values of :class:`PairMixin`.'''
        if self.cache.cache is None:
            data = yield self.read_backend_structure().values()
            yield self.load_values(data)
        else:
            yield self.cache.cache.values()
        
    def pair(self, pair):
        '''Add a *pair* to the structure.'''
        if len(pair) == 1:
            # if only one value is passed, the value must implement a
            # score function which retrieve the first value of the pair
            # (score in zset, timevalue in timeseries, field value in hashtable)
            return (pair[0].score(), pair[0])
        elif len(pair) != 2:
            raise TypeError('add expected 2 arguments, got {0}'\
                            .format(len(pair)))
        else:
            return pair
        
    def add(self, *pair):
        return self.update((pair,))
        
    @commit_when_no_transaction
    def update(self, mapping):
        '''Add *mapping* dictionary to hashtable.
Equivalent to python dictionary update method.

:parameter mapping: a dictionary of field values.'''
        self.cache.update(self.dump_data(mapping))

    def dump_data(self, mapping):
        tokey = self.pickler.dumps
        dumps = self.value_pickler.dumps
        if isinstance(mapping, dict):
            mapping = iteritems(mapping)
        p = self.pair
        data = []
        for pair in mapping:
            if not isinstance(pair, tuple):
                pair = pair,
            k, v = p(pair)
            data.append((tokey(k), dumps(v)))
        return data
    
    def load_data(self, mapping):
        loads = self.pickler.loads
        if self.value_pickler.require_session():
            data1 = []
            def _iterable():
                for k, v in iterpair(mapping):
                    data1.append(loads(k))
                    yield v
            res = self.value_pickler.load_iterable(_iterable(), self.session)
            return on_result(res, lambda data2: zip(data1, data2))
        else:
            vloads = self.value_pickler.loads
            return [(loads(k), vloads(v)) for k, v in iterpair(mapping)]
    
    def load_keys(self, iterable):
        loads = self.pickler.loads
        return [loads(k) for k in iterable]
    
    def load_values(self, iterable):
        vloads = self.value_pickler.loads
        return [vloads(v) for v in iterable]

    
class KeyValueMixin(PairMixin):
    '''A mixin for ordered and unordered key-valued pair containers.
A key-value pair container has the :meth:`values` and :meth:`items`
methods, while its iterator is over keys.'''
    def __iter__(self):
        res = self.keys()
        if is_async(res):
            raise RuntimeError('Cannot iterate on asynchronous result.')
        else:
            return iter(res)
    
    @async()
    def keys(self):
        if self.cache.cache is None:
            keys = yield self.read_backend_structure().keys()
            yield self.load_keys(keys)
        else:
            yield self.cache.cache
    
    def __delitem__(self, key):
        '''Remove an element. Same as the :meth:`remove` method`.'''
        return self.pop(key)

    def __getitem__(self, key):
        dkey = self.pickler.dumps(key)
        if self.cache.cache is None:
            res = self.read_backend_structure().get(dkey)
            return on_result(res, lambda r: self._load_get_data(r, key))
        else:
            return self.cache.cache[key]
    
    def get(self, key, default=None):
        '''Retrieve a single element from the structure.
If the element is not available return the default value.

:parameter key: lookup field
:parameter default: default value when the field is not available'''
        if self.cache.cache is None:
            dkey = self.pickler.dumps(key)
            res = self.read_backend_structure().get(dkey)
            return on_result(res, lambda r: self._load_get_data(r, key, default))
        else:
            return self.cache.cache.get(key, default)
        
    def pop(self, key, *args):
        if len(args) <= 1:
            dkey = self.pickler.dumps(key)
            res = self.read_backend_structure().pop(dkey)
            return on_result(res, lambda r: self._load_get_data(r, key, *args))
        else:
            raise TypeError('pop expected at most 2 arguments, got {0}'\
                            .format(len(args)+1))
        
    @commit_when_no_transaction
    def remove(self, *keys):
        '''Remove *keys* from the key-value container.'''
        dumps = self.pickler.dumps
        self.cache.remove([dumps(v) for v in keys])
    
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
a numeric value, the score.'''
    
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
    
    def range(self, start, stop, callback=None, withscores=True, **options):
        '''Return a range with scores between start and end.'''
        s1 = self.pickler.dumps(start)
        s2 = self.pickler.dumps(stop)
        res = self.backend_structure().range(s1, s2, withscores=withscores,
                                             **options)
        if not callback:
            callback = self.load_data if withscores else self.load_values
        return on_result(res, callback)
    
    def irange(self, start=0, end=-1, callback=None, withscores=True, **options):
        '''Return the range by rank between start and end.'''
        res = self.backend_structure().irange(start, end, withscores=withscores,
                                              **options)
        if not callback:
            callback = self.load_data if withscores else self.load_values
        return on_result(res, callback)
        
    def pop_range(self, start, stop, callback=None, withscores=True):
        '''pop a range by score from the :class:`OrderedMixin`'''
        s1 = self.pickler.dumps(start)
        s2 = self.pickler.dumps(stop)
        res = self.backend_structure().pop_range(s1, s2, withscores=withscores)
        if not callback:
            callback = self.load_data if withscores else self.load_values
        return on_result(res, callback)

    def ipop_range(self, start=0, stop=-1, callback=None, withscores=True):
        '''pop a range from the :class:`OrderedMixin`'''
        res = self.backend_structure().ipop_range(start, stop,
                                                  withscores=withscores)
        if not callback:
            callback = self.load_data if withscores else self.load_values
        return on_result(res, callback)
    

class Sequence(object):
    '''Mixin for a :class:`Structure` which implements a kind of sequence
container. The elements in a sequence container are ordered following a linear
sequence.'''
    cache_class = listcache
    
    @async()
    def items(self):
        if self.cache.cache is None:
            data = yield self.read_backend_structure().range()
            self.cache.set_cache(self.load_data(data))
        yield self.cache.cache
    
    @commit_when_no_transaction
    def push_back(self, value):
        '''Appends a copy of *value* at the end of the :class:`Sequence`.'''
        self.cache.push_back(self.value_pickler.dumps(value))
        return self
    
    @async()    
    def pop_back(self):
        '''Remove the last element from the :class:`Sequence`.'''
        value = yield self.backend_structure().pop_back()
        if value is not None:
            yield self.value_pickler.loads(value)
    
    def __getitem__(self, index):
        value = self.read_backend_structure().get(index)
        return on_result(value, self.value_pickler.loads)
    
    def __setitem__(self, index, value):
        value = self.value_pickler.dumps(value)
        self.backend_structure().set(index, value)
    
    
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
    @async()
    def pop_front(self):
        '''Remove the first element from of the list.'''
        value = yield self.backend_structure().pop_front()
        if value is not None:
            yield self.value_pickler.loads(value)
    
    @async()
    def block_pop_back(self, timeout=10):
        '''Remove the last element from of the list. If no elements are
available, blocks for at least ``timeout`` seconds.'''
        value = yield self.backend_structure().block_pop_back(timeout)
        if value is not None:
            yield self.value_pickler.loads(value)
    
    @async()
    def block_pop_front(self, timeout=10):
        '''Remove the first element from of the list. If no elements are
available, blocks for at least ``timeout`` seconds.'''
        value = yield self.backend_structure().block_pop_front(timeout)
        if value is not None:
            yield self.value_pickler.loads(value)
    
    @commit_when_no_transaction
    def push_front(self, value):
        '''Appends a copy of ``value`` to the beginning of the list.'''
        self.cache.push_front(self.value_pickler.dumps(value))


class Zset(OrderedMixin, PairMixin, Set):
    '''An ordered version of :class:`Set`. It derives from
:class:`OrderedMixin` and :class:`PairMixin`.'''
    pickler = encoders.Double()
    cache_class = zsetcache
    
    def rank(self, value):
        '''The rank of a given *value*. This is the position of *value*
in the :class:`OrderedMixin` container.'''
        value = self.value_pickler.dumps(value)
        return self.backend_structure().rank(value)
                    
    
class HashTable(KeyValueMixin, Structure):
    '''A :class:`Structure` which is the networked equivalent to
a Python ``dict``. Derives from :class:`KeyValueMixin`.'''
    pickler = encoders.Default()
    cache_class = hashcache
    
    if not ispy3k:
        def iteritems(self):
            return self.items()
        
        def itervalues(self):
            return self.values()

    
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
        res = self.backend_structure().ipop(index)
        return on_result(res, lambda r: self._load_get_data(r, index, None))
    
    def times(self, start, stop, callback=None, **kwargs):
        '''The times between times *start* and *stop*.'''
        s1 = self.pickler.dumps(start)
        s2 = self.pickler.dumps(stop)
        res = self.backend_structure().times(s1, s2, **kwargs)
        return on_result(res, callback or self.load_keys)
    
    def itimes(self, start=0, stop=-1, callback=None, **kwargs):
        '''The times between rank *start* and *stop*.'''
        res = self.backend_structure().itimes(start, stop, **kwargs)
        return on_result(res, callback or self.load_keys)
    
    
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
    