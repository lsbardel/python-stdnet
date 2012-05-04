from uuid import uuid4
from collections import namedtuple

from stdnet.utils import iteritems, itervalues, missing_intervals, encoders,\
                            BytesIO
from stdnet.lib import zset, skiplist
from stdnet import SessionNotAvailable

from .base import ModelBase
from .session import commit_when_no_transaction, withsession


__all__ = ['Structure',
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
    '''Base class for remote data-structures. Remote structures are the
backend of :ref:`structured fields <model-field-structure>` but they
can also be used as stand alone objects. For example::

    import stdnet
    db = stdnet.getdb(...)
    mylist = db.list('bla')
    
:parameter backend: instance of the remote :class:`BackendDataServer` where
    the structure is stored.
:parameter id: structure id
:parameter pipetype: check the :attr:`pipetype`. Specified by the server.
:parameter instance: Optional :class:`stdnet.odm.StdModel` instance to which
    the structure belongs to via a
    :ref:`structured field <model-field-structure>`.
    This field is specified when accessing remote structures via the object
    relational mapper.

.. attribute:: instance

    An optional :class:`StdModel` instance to which
    the structure belongs to via a
    :ref:`structured field <model-field-structure>`.
    
    Defaulr ``None``.
    
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
    def __init__(self, instance = None, timeout = 0, value_pickler = None,
                 **kwargs):
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
    def backend_structure(self, client = None):
        return self.session.backend.structure(self, client)


################################################################################
##    Mixins Structures
################################################################################

class PairMixin(object):
    '''A mixin for handling structures with which holds pairs.'''
    pickler = encoders.NoEncoder()
    
    def setup(self, pickler = None, **kwargs):
        self.pickler = pickler or self.pickler
        
    @withsession
    def __getitem__(self, key):
        key = self.pickler.dumps(key)
        result = self.session.backend.structure(self).get(key)
        if result is None:
            raise KeyError(key)
        return self.value_pickler.loads(result)
    
    def __setitem__(self, key, value):
        self.add(key, value)

    def get(self, key, default = None):
        '''Retrieve a single element from the structure.
If the element is not available return the default value.

:parameter key: lookup field
:parameter default: default value when the field is not available.
:parameter transaction: an optional transaction instance.
:rtype: a value in the hashtable or a pipeline depending if a
    transaction has been used.'''
        try:
            return self.__getitem__(key)
        except KeyError:
            return default
    
    @withsession
    def _iter(self):
        loads = self.pickler.loads
        for k in self.session.structure(self):
            yield loads(k)
    
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
        if isinstance(mapping,dict):
            mapping = iteritems(mapping)
        return ((loads(k),vloads(v)) for k,v in mapping)
    

class KeyValueMixin(PairMixin):
    '''A mixin for key-valued pair containes.'''
    def __delitem__(self, key):
        '''Immediately remove an element. To remove with transactions use the
:meth:`remove` method`.'''
        self._pop(key)
            
    def pop(self, key, *args):
        if len(args) > 1:
            raise TypeError('pop expected at most 2 arguments, got {0}'\
                            .format(len(args)+1))
        try:
            return self._pop(key)
        except KeyError:
            if args:
                return args[0]
            else:
                raise
        
    @commit_when_no_transaction
    def remove(self, *keys):
        '''Remove *keys* from the key-value container.'''
        dumps = self.pickler.dumps
        self.cache.remove([dumps(v) for v in keys])
        
    @withsession
    def values(self):
        vloads = self.value_pickler.loads
        for v in self.session.structure(self).values():
            yield vloads(v)
            
    # PRIVATE
    
    def _pop(self, key):
        k = self.pickler.dumps(key)
        v = self.session.structure(self).pop(k)
        if v is None:
            raise KeyError(key)
        else:
            self.cache.remove((k,),False)
            return self.value_pickler.loads(v)
    
    
class OrderedMixin(object):
    '''A mixin for :class:`Structure` wich maintain ordering with respect
a float value.'''
    
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
        
    def rank(self, value):
        value = self.pickler.dumps(value)
        return self.backend_structure().rank(value)
    
    def range(self, start, stop, callback = None, **kwargs):
        s1 = self.pickler.dumps(start)
        s2 = self.pickler.dumps(stop)
        res = self.backend_structure().range(s1, s2, **kwargs)
        return self.async_handle(res, callback or self.load_data)
    
    def irange(self, start = 0, end = -1, callback = None, **kwargs):
        '''Return a range between start and end key.'''
        res = self.backend_structure().irange(start, end, **kwargs)
        return self.async_handle(res, callback or self.load_data)


class Sequence(object):
    cache_class = listcache
    
    @commit_when_no_transaction
    def push_back(self, value):
        '''Appends a copy of *value* to the end of the list.'''
        self.cache.push_back(self.value_pickler.dumps(value))
        return self
        
    def pop_back(self):
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
    '''A linked-list :class:`stdnet.Structure`.'''
    
    def pop_front(self):
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
    '''An ordered version of :class:`Set`.'''
    pickler = encoders.Double()
    cache_class = zsetcache
    
    def _iter(self):
        # Override the KeyValueMixin so that it iterates over values rather
        # scores
        loads = self.value_pickler.loads
        for v in self.session.structure(self):
            yield loads(v)
            
    def ipop(self, start, stop = None, **options):
        '''pop a range from the set'''
        return self.backend_structure().ipop(start, stop, **options)
        
    def pop(self, start, stop = None, **options):
        '''pop a score range from the set'''
        return self.backend_structure().pop(start, stop, **options)
            
    
class HashTable(KeyValueMixin, Structure):
    '''A hash-table :class:`stdnet.Structure`.
The networked equivalent to a Python ``dict``.'''
    cache_class = hashcache
    
    def addnx(self, field, value, transaction = None):
        '''Set the value of a hash field only if the field
does not exist.'''
        return self._addnx(self.cursor(transaction),
                           self.pickler.dumps(key),
                           self.pickler_value.dumps(value))

    
class TS(OrderedMixin, KeyValueMixin, Structure):
    '''A timeseries :class:`Structure`. This is an experimental structure
not available with vanilla redis. Check the
:ref:`timeseries documentation <apps-timeserie>` for further information.'''
    pickler = encoders.DateTimeConverter()
    value_pickler = encoders.Json()
    cache_class = tscache 
    
    
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
    