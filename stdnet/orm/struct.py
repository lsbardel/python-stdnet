from uuid import uuid4
from collections import namedtuple

from stdnet.utils import iteritems, itervalues, missing_intervals, encoders
from stdnet.lib import zset, nil

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
        self.back = []
        self.front = []
        
    
class setcache(object):
    
    def __init__(self):
        self.toadd = set()
        self.toremove = set()

    def __contains__(self, v):
        return v in self.toadd
    
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
        self.toadd.clear()
        self.toremove.clear()
        

class zsetcache(setcache):
    
    def __init__(self):
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
        self.remote = {}
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
    
    def _(self, *args, **kwargs):
        if self.session:
            return f(self, *args, **kwargs)
        else:
            raise ValueError('Cannot perform operation. No session available')

    _.__name__ = f.__name__
    _.__doc__ = f.__doc__        
    return _

            
class ResultCallback(object):
    __slots__ = ('default','loads')
    
    def __init__(self, default, loads):
        self.default = default
        self.loads = loads
    
    def __call__(self, res):
        if res and res is not nil:
            if self.loads:
                return self.loads(res)
            else:
                return res
        else:
            if isinstance(self.default,Exception):
                raise self.default
            else:
                return self.default


class KeyValueCallback(object):
    __slots__ = ('loads','vloads')
    
    def __init__(self, loads, vloads):
        self.loads = loads
        self.vloads = vloads
    
    def __call__(self, result):
        kloads = self.loads
        vloads = self.vloads
        return ((kloads(k),vloads(v)) for k,v in result)


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
    
.. attribute:: scorefun

    A callable which takes a value as parameter and return a float
    number to be usedto score the value. Used in :class:`OrderedSet` structure.
    
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
        loads = self.value_pickler.loads
        for v in self.session.structure(self):
            yield loads(v)
                
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
        return session.backend.structure_contains(self)
    
    def __len__(self):
        return self.size()
            
    # PURE VIRTUAL METHODS
    
    def _has(self, cursor, value):
        raise NotImplementedError()
    
    def _all(self, cursor):
        raise NotImplementedError
    
    def _size(self, cursor):
        raise NotImplementedError
    
    def _remove(self, cursor, items):
        # Remove items form the structures
        raise NotImplementedError
    
    def _delete(self, cursor):
        '''Delete structure from remote backend.'''
        raise NotImplementedError
    
    def _save(self, cursor, pipeline):
        raise NotImplementedError("Could not save")
    
    def add_expiry(self, cursor, timeout):
        '''Internal method called if a timeout is set.
This needs to implemented.'''
        raise NotImplementedError("Could not save")


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
        
    def __iter__(self):
        loads = self.value_pickler.loads
        for s,v in self.session.structure(self):
            yield float(s),loads(v)
        
    def set_cache(self, r):
        loads = self.pickler.loads
        self._cache = list((loads(v) for v in r))
        
    def add(self, score, value):
        '''Add *value* to the set'''
        score = float(score)
        self.cache.add(score,self.value_pickler.dumps(value))

    def update(self, values, transaction = None):
        '''Add iterable *values* to the set'''
        d = self.value_pickler.dumps
        add = self.cache.add
        for score,value in values:
            add(float(score),d(value))
            
    def rank(self, value):
        if self.pickler:
            value = self.pickler.dumps(value)
        return self._rank(value)
    
    def range(self, start, stop):
        if self.pickler:
            value = self.pickler.dumps(value)
        return self._rank(value)
        
    # VIRTUAL FUNCTIONS
    
    def range(self, start, end = -1, withscores = False):
        raise NotImplementedError
    
    def _rank(self, value):
        raise NotImplementedError

    
class HashTable(Structure):
    '''A hash-table :class:`stdnet.Structure`.
The networked equivalent to a Python ``dict``.'''
    pickler = None
    cache_class = hashcache
    
    def setup(self, pickler = None, **kwargs):
        self.pickler = pickler or self.pickler or encoders.Default()
        
    def __iter__(self):
        loads = self.pickler.loads
        for k in self.session.structure(self):
            yield loads(v)

    def values(self):
        vloads = self.value_pickler.loads
        for v in self.session.structure(self).values():
            yield vloads(v)
                        
    def items(self):
        loads = self.pickler.loads
        vloads = self.value_pickler.loads
        for k,v in self.session.structure(self).items():
            yield loads(k),vloads(v)
            
    def set_cache(self, items):
        kloads = self.pickler.loads
        vloads = self.value_pickler.loads
        self._cache = dict(((kloads(k),vloads(v)) for k,v in items))
        
    def keyvaluedata(self, func, transaction, *args, **kwargs):
        '''invoke remote function in the server and if
we are not using a pipeline, unpickle the result.'''
        rk = KeyValueCallback(self.pickler.loads,
                              self.value_pickler.loads)
        if transaction:
            transaction.add(func, args, kwargs, callback = rk)
        else:
            return rk(func(self.backend.cursor(), *args, **kwargs))
        
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
    
    def update(self, mapping):
        '''Add *mapping* dictionary to hashtable.
Equivalent to python dictionary update method.

:parameter mapping: a dictionary of field values.'''
        tokey = self.pickler.dumps
        dumps = self.value_pickler.dumps
        if isinstance(mapping,dict):
            mapping = iteritems(mapping)
        self.cache.update(dict(((tokey(k),dumps(v)) for k,v in mapping)))
    
    def get(self, key, default = None):
        '''Retrieve a single element from the hashtable.
If the element is not available return the default value.

:parameter key: lookup field
:parameter default: default value when the field is not available.
:parameter transaction: an optional transaction instance.
:rtype: a value in the hashtable or a pipeline depending if a
    transaction has been used.'''
        key = self.pickler.dumps(key)
        result = self.session.backend.structure(self).get(key,default)
        return self.value_pickler.loads(result[0])
    
    def __getitem__(self, key):
        v = self.get(key)
        if v is None:
            raise KeyError('%s not available' % key)
        else:
            return v

    def items(self, keys = None):
        '''Generator over key-values.
If keys is not supplied, it is a generator over all key-value items.
No transaction involved in this function.'''
        if self.cache:
            if self.keys:
                cache = self.cache.get
                for key in keys:
                    yield key,cache(key)
            else:
                for item in iteritems(self.cache):
                    yield item
        else:
            kloads = self.pickler.loads
            vloads = self.value_pickler.loads
            if keys:
                dumps = self.pickler.dumps
                keys = [dumps(k) for k in keys]
                items = zip(keys,self._items(self.backend.cursor(),keys))
                for key,val in items:
                    yield kloads(key),vloads(val)
            else:
                cache = {}
                items = self._items(self.backend.cursor(),keys)
                for key,val in items:
                    k,v = kloads(key),vloads(val)
                    cache[k] = v
                    yield k,v
                self._cache = cache
            
    def values(self, keys = None):
        '''Generator overvalues.
If keys is not supplied, it is a generator over value items.
No transaction involved in this function.'''
        if self.cache:
            if self.keys:
                cache = self.cache.get
                for key in keys:
                    yield cache(key)
            else:
                for item in itervalues(self.cache):
                    yield item
        else:
            kloads = self.pickler.loads
            vloads = self.value_pickler.loads
            if keys:
                dumps = self.pickler.dumps
                keys = [dumps(k) for k in keys]
                for item in self._items(self.backend.cursor(),keys):
                    yield vloads(item)
            else:
                for key,val in self._items(self.backend.cursor(),keys):
                    yield vloads(val)
            
    def range(self, start, end, desc = False):
        '''Return a generator of ordered items between start and end.'''
        items = sorted(self.items(),key = lambda t : t[0])
        if not desc:
            items = reversed(items)
        return items
            
    def values(self):
        for key,value in self.items():
            yield value
    
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
    
    def _all(self, cursor):
        return self._items(cursor, None)
    
    # PURE VIRTUAL METHODS
    
    def _addnx(self, field, value):
        raise NotImplementedError
    
    def _contains(self, cursor, value):
        raise NotImplementedError
    
    def _get(self, cursor, key):
        raise NotImplementedError
    
    def _items(self, cursor, keys):
        raise NotImplementedError
    
    def _pop(self, cursor, key):
        raise NotImplementedError

    
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
            
    # PURE VIRTUAL METHODS
    
    def _count(self, cursor, start, end):
        raise NotImplementedError
    
    def _range(self, cursor, start, end):
        raise NotImplementedError
    
    def _irange(self, cursor, start, end):
        raise NotImplementedError
    
    def _front(self, cursor):
        raise NotImplementedError
    
    def _back(self, cursor):
        raise NotImplementedError
       


class make_struct(object):
    
    def __init__(self, name, pickler = None, value_pickler = None):
        self._name = name
        self._pickler = pickler
        self._value_pickler = value_pickler
    
    def __call__(self, id = None, pickler = None, value_pickler = None,
                 instance = None, **kwargs):
        pickler = pickler or self._pickler
        value_pickler = value_pickler or self._value_pickler
        s = st(self._id(id),
               pickler = pickler or self._pickler,
               value_pickler = value_pickler or self._value_pickler,
               instance = instance,
               **kwargs)
        return s
        
    def _id(self, id):
        return id or 'stdnet-{0}'.format(str(uuid4())[:8])
    
    @classmethod
    def clear(cls, ids):
        if ids:
            for id in ids:
                s = self._structs.pop(id,None)
                if s:
                    try:
                        s.delete()
                    except ConnectionError:
                        pass
        else:
            for s in itervalues(cls._structs):
                try:
                    s.delete()
                except ConnectionError:
                    pass
            cls._structs.clear()


Dict = make_struct(HashTable, value_pickler = encoders.PythonPickle())