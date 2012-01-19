from uuid import uuid4
from collections import namedtuple

from stdnet.utils import iteritems, itervalues, missing_intervals, encoders
from stdnet.lib import zset, nil
from stdnet import SessionNotAvailable

from .base import ModelBase


__all__ = ['Structure',
           'List',
           'Set',
           'Zset',
           'HashTable',
           'TS']


default_score = lambda x : x


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
    
    def add(self, v):
        self.toadd.add(v)
        self.toremove.discard(v)
        
    def update(self, v):
        self.toadd.update(v)
        self.toremove.difference_update(v)
        
    def difference_update(self, v):
        self.toadd.difference_update(v)
        self.toremove.update(v)
        
    def discard(self, v):
        self.toadd.discard(v)
        self.toremove.add(v)
        
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

    def add(self, score, v):
        self.toadd.add(score,v)
        self.toremove.discard(v)
        
    def update(self, v):
        self.toadd.update(v)
        self.toremove.difference_update(v)
        
    
class hashcache(setcache):
    
    def __init__(self):
        self.cache = {}
        self.toadd = {}
        self.toremove = {}

    def update(self, v):
        self.toadd.update(v)
        for k in v:
            self.toremove.pop(v,None)


class tscache(object):
    
    def __init__(self):
        self.fields = {}
        self.delete_fields = set()
        self.deleted_timestamps = set()

    def add(self, timestamp, field, value):
        if field not in self.fields:
            self.fields[field] = skiplist()
        self.fields[field].insert(timestamp,value)
        
    def flush(self):
        self.fields.clear()
        self.delete_fields.clear()
        self.deleted_timestamps.clear()
        
    def flat(self):
        if self.deleted_timestamps or self.delete_fields or self.fields:
            args = [len(self.deleted_timestamps)]
            args.extend(self.deleted_timestamps)
            args.append(len(self.delete_fields))
            args.extend(self.delete_fields)
            for field in self.fields:
                val = self.fields[field]
                args.append(field)
                args.append(len(val))
                args.extend(val.flat())
            self.flush()
            return args
        
        
def withsession(f):
    '''Decorator for instance methods which require a :class:`Session`
available to perform their call. If a session is not available. It raises
a :class:`SessionNotAvailable` exception.
'''
    def _(self, *args, **kwargs):
        if self.session:
            return f(self, *args, **kwargs)
        else:
            raise SessionNotAvailable(
                        'Cannot perform operation. No session available')

    _.__name__ = f.__name__
    _.__doc__ = f.__doc__        
    return _


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
:parameter instance: Optional :class:`stdnet.orm.StdModel` instance to which
    the structure belongs to via a
    :ref:`structured field <model-field-structure>`.
    This field is specified when accessing remote structures via the object
    relational mapper.

.. attribute:: instance

    An optional :class:`stdnet.orm.StdModel` instance to which
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
    value_pickler = None
    def __init__(self, instance = None, timeout = 0, value_pickler = None,
                 **kwargs):
        self.instance = instance
        self.value_pickler = value_pickler or self.value_pickler or\
                                encoders.NumericDefault()
        self.timeout = timeout
        self.setup(**kwargs)
        if self.instance is not None:
            self.session = instance.session
        
    def makeid(self):
        return '{0}.{1}'.format(self._meta,str(uuid4())[:8])
        
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
    def delete(self):
        '''Delete the structure from the remote backend. If a transaction is
specified, the data is pipelined and executed when the transaction completes.'''
        self.session.delete(self)
        
    @withsession
    def size(self):
        '''Number of elements in structure. If no transaction is
supplied, use the backend default cursor.'''
        return self.session.backend.structure(self).size()
    
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
        loads = self.value_pickler.loads
        return (loads(v) for v in data)


class List(Structure):
    '''A linked-list :class:`stdnet.Structure`.'''
    cache_class = listcache
        
    def pop_back(self, transaction = None):
        return self._unpicklefrom(self._pop_back, transaction, None,
                                  self.pickler.loads)
    
    def pop_front(self, transaction = None):
        return self._unpicklefrom(self._pop_front, transaction, None,
                                  self.pickler.loads)
    
    def block_pop_back(self, timeout = None, transaction = None):
        return self._unpicklefrom(self._block_pop_back, transaction, None,
                                  lambda x : self.pickler.loads(x[1]),
                                  timeout)
    
    def block_pop_front(self, timeout = None, transaction = None):
        return self._unpicklefrom(self._block_pop_front, transaction, None,
                                  lambda x : self.pickler.loads(x[1]),
                                  timeout)
    
    def push_back(self, value):
        '''Appends a copy of *value* to the end of the list.'''
        self.cache.push_back(self.value_pickler.dumps(value))
    
    def push_front(self, value):
        '''Appends a copy of *value* to the beginning of the list.'''
        self.cache.push_front(self.value_pickler.dumps(value))


class Set(Structure):
    '''An unordered set :class:`Structure`. Equivalent to a python ``set``.'''
    cache_class = setcache
     
    def add(self, value):
        '''Add *value* to the set'''
        self.cache.add(self.value_pickler.dumps(value))

    def update(self, values):
        '''Add iterable *values* to the set'''
        d = self.value_pickler.dumps
        self.cache.update(tuple((d(v) for v in values)))
            
    def discard(self, value):
        '''Remove an element *value* from a set if it is a member.'''
        self.cache.discard(self.value_pickler.dumps(value))
    remove = discard
        
    def difference_update(self, values):
        '''Remove an iterable of *values* from the set.'''
        d = self.value_pickler.dumps
        self.cache.difference_update(tuple((d(v) for v in values)))


class Zset(Set):
    '''An ordered version of :class:`Set`.'''
    cache_class = zsetcache
        
    def _iter(self):
        loads = self.value_pickler.loads
        for s,v in self.session.structure(self):
            yield float(s),loads(v)
        
    def load_data(self, mapping):
        if isinstance(mapping,dict):
            mapping = iteritems(mapping)
        loads = self.value_pickler.loads
        return ((s,loads(v)) for s,v in mapping)
        
    def add(self, score, value):
        '''Add *value* to the set'''
        score = float(score)
        self.cache.add(score,self.value_pickler.dumps(value))

    def update(self, mapping):
        '''Add iterable *values* to the set'''
        if isinstance(mapping,dict):
            mapping = iteritems(mapping)
        d = self.value_pickler.dumps
        add = self.cache.add
        for score,value in mapping:
            add(float(score),d(value))
            
    def rank(self, value):
        value = self.pickler.dumps(value)
        return self.session.structure(self).rank(value)
    
    def range(self, start, stop):
        if self.pickler:
            value = self.pickler.dumps(value)
        return self._rank(value)
        
    
class HashTable(Structure):
    '''A hash-table :class:`stdnet.Structure`.
The networked equivalent to a Python ``dict``.'''
    pickler = None
    cache_class = hashcache
    
    def setup(self, pickler = None, **kwargs):
        self.pickler = pickler or self.pickler or encoders.Default()
        
    @withsession
    def __getitem__(self, key):
        key = self.pickler.dumps(key)
        result = self.session.backend.structure(self).get(key)
        if result is None:
            raise KeyError(key)
        return self.value_pickler.loads(result)
            
    @withsession
    def __iter__(self):
        loads = self.pickler.loads
        for k in self.session.structure(self):
            yield loads(k)

    @withsession
    def values(self):
        vloads = self.value_pickler.loads
        for v in self.session.structure(self).values():
            yield vloads(v)
    
    @withsession        
    def items(self):
        loads = self.pickler.loads
        vloads = self.value_pickler.loads
        for k,v in self.session.structure(self).items():
            yield loads(k),vloads(v)
        
    def __delitem__(self, key):
        self.pop(key)
        
    def pop(self, key, *args):
        if args:
            if len(args) > 1:
                raise TypeError('pop expected at most 2 arguments, got {0}'\
                                .format(len(args)+1))
            default = args[0]
        else:
            default = KeyError(key)
        key = self.pickler.dumps(key)
        v = self.session.structure(self).pop(key,default)
        return self.pickler_value.loads(v)
        
    def add(self, key, value):
        '''Add ``key`` - ``value`` pair to hashtable.'''
        self.update(((key,value),))
    __setitem__ = add
    
    def addnx(self, field, value, transaction = None):
        '''Set the value of a hash field only if the field
does not exist.'''
        return self._addnx(self.cursor(transaction),
                           self.pickler.dumps(key),
                           self.pickler_value.dumps(value))
    
    def __setitem__(self, key, value):
        return self.update(((key,value),))
    
    def load_data(self, mapping):
        loads = self.pickler.loads
        vloads = self.value_pickler.loads
        if isinstance(mapping,dict):
            mapping = iteritems(mapping)
        return ((loads(k),vloads(v)) for k,v in mapping)
    
    def dump_data(self, mapping):
        tokey = self.pickler.dumps
        dumps = self.value_pickler.dumps
        if isinstance(mapping,dict):
            mapping = iteritems(mapping)
        return ((tokey(k),dumps(v)) for k,v in mapping)
        
    def update(self, mapping):
        '''Add *mapping* dictionary to hashtable.
Equivalent to python dictionary update method.

:parameter mapping: a dictionary of field values.'''
        self.cache.update(self.dump_data(mapping))
    
    def get(self, key, default = None):
        '''Retrieve a single element from the hashtable.
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
            
    def range(self, start, end, desc = False):
        '''Return a generator of ordered items between start and end.'''
        items = sorted(self.items(),key = lambda t : t[0])
        if not desc:
            items = reversed(items)
        return items
            
    def sortedkeys(self, desc = True):
        keys = sorted(self)
        if not desc:
            keys = reversed(keys)
        return keys
            
    def sorteditems(self, desc = True):
        items = sorted(self.items(),key = lambda t : t[0])
        if not desc:
            items = reversed(items)
        return items   

    
class TS(HashTable):
    '''A timeseries :class:`Structure`. This is an experimental structure
not available with vanilla redis. Check the
:ref:`timeseries documentation <apps-timeserie>` for further information.'''
    pickler = encoders.DateTimeConverter()
    value_pickler = encoders.Json()
    
    def set_cache(self, r):
        kloads = self.pickler.loads
        vloads = self.value_pickler.loads
        self._cache = list(((kloads(k),vloads(v)) for k,v in r))
        
    def front(self, transaction = None):
        '''Return the front key of timeseries'''
        return self._unpicklefrom(self._front, transaction, None,
                                  self.pickler.loads)
        
    def back(self, transaction = None):
        '''Return the back key of timeseries'''
        return self._unpicklefrom(self._back, transaction, None,
                                  self.pickler.loads)
        
    def range(self, start, end, transaction = None):
        '''Return a generator of a range between start and end key.'''
        tokey = self.pickler.dumps
        return self.keyvaluedata(self._range,transaction,
                                 tokey(start),
                                 tokey(end))
            
    def irange(self, start = 0, end = -1, transaction = None):
        '''Return a range between start and end key.'''
        return self.keyvaluedata(self._irange,transaction,
                                 start, end)
            
    def count(self, start, end):
        tokey    = self.pickler.dumps
        return self._count(self.backend.cursor(),
                           tokey(start),tokey(end))
       
