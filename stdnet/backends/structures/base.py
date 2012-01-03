'''Interfaces for supported data-structures'''
from collections import namedtuple

import stdnet
from stdnet.utils import iteritems, itervalues, missing_intervals, encoders
from stdnet.lib import zset, nil

__all__ = ['PipeLine',
           'Structure',
           'List',
           'Set',
           'OrderedSet',
           'HashTable',
           'TS']


default_score = lambda x : x

        
class listPipeline(object):
    def __init__(self):
        self.clear()
        
    def push_front(self, value):
        self.front.append(value)
        
    def push_back(self, value):
        self.back.append(value)
        
    def clear(self):
        self.back = []
        self.front = []
        
    def __len__(self):
        return len(self.back) + len(self.front)
    

class PipeLine(object):
    '''A pipeline utility class. Used to hold data in :class:`stdnet.Structure`
 before it is saved into the data backend.'''
    def __init__(self, pipe, method, timeout):
        self.pipe = pipe
        self.method = method
        self.timeout = timeout
        
    def __repr__(self):
        return self.pipe.__repr__()
        
        
class HashPipe(PipeLine):
    def __init__(self, timeout = 0):
        super(HashPipe,self).__init__({},'hash',timeout)
        
        
class TsPipe(PipeLine):
    def __init__(self, timeout = 0):
        super(TsPipe,self).__init__({},'ts',timeout)


class SetPipe(PipeLine):
    def __init__(self, timeout = 0):
        super(SetPipe,self).__init__(set(),'unordered_set',timeout)

        
class OsetPipe(PipeLine):
    def __init__(self, timeout = 0):
        super(OsetPipe,self).__init__(zset(),'ordered_set',timeout)        

    
class ListPipe(PipeLine):
    def __init__(self, timeout = 0):
        super(ListPipe,self).__init__(listPipeline(),'list',timeout)
        

_pipelines = {'list':ListPipe,
              'hash': HashPipe,
              'ts': TsPipe,
              'unordered_set': SetPipe,
              'ordered_set': OsetPipe}


structure_meta = namedtuple('structure_meta','cursor')


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


class Structure(object):
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

        
.. attribute:: server

    instance of a :class:`stdnet.BackendDataServer`.
    
.. attribute:: pipetype

    one of ``list``, ``hash``, ``ordered_set``, ``unordered_set``, ``ts``.
    
.. attribute:: id

    unique *id* for the structure
    
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
    pickler = None
    value_pickler = None
    def __init__(self, backend, id,  pipetype, instance = None,
                 timeout = 0, pickler = None,
                 value_pickler = None, scorefun = None,
                 **kwargs):
        self.pipetype = pipetype
        self.backend = backend
        self.scorefun = scorefun or default_score
        self.instance = instance
        self.pickler = pickler or self.pickler or backend.pickler
        self.value_pickler = value_pickler or self.value_pickler or\
                                 backend.pickler
        self.id = id
        self._cache = None
        self.timeout = timeout
    
    @property
    def struct(self):
        return _pipelines[self.pipetype]
    
    @property
    def cache(self):
        return self._cache
    
    def pipe(self, transaction = None):
        '''Return a structure pipe given a *transaction*.
The pipe is the :attr:`Pipeline.pipe` attribute of the structure pipeline.

:parameter transaction: Optional :class:`stdnet.Transaction` instance.
:rtype: a local strcuture to hold data before it is safe into
    the backend database.'''
        return transaction.structure_pipe(self) if transaction\
             else self._pipe()
        
    def cursor(self, transaction = None):
        '''Return the backend cursor given a *transaction*.'''
        return transaction.cursor if transaction else self._cursor()
        
    def __repr__(self):
        base = '%s:%s' % (self.__class__.__name__,self.id)
        if self._cache is None:
            return base
        else:
            return '%s %s' % (base,self._cache)
        
    def __str__(self):
        return self.__repr__()
    
    def __hash__(self):
        return hash(self.id)
    
    def __iter__(self):
        if self._cache is None:
            cache = []
            loads = self.pickler.loads
            for item in self._all(self.backend.cursor()):
                item = loads(item)
                cache.append(item)
                yield item
            self._cache = cache
        else:
            for item in self.cache:
                yield item
                
    def reload(self, transaction = None):
        '''Fill data from the backend'''
        return self._all(self.cursor(transaction))

    def save(self, transaction = None):
        '''Save the data in the back-end server. If a transaction is
specified, the data is pipelined and executed when the transaction completes.'''
        cursor = transaction.cursor if transaction else self.backend.cursor()
        return self._save_from_pipeline(cursor, self.pipe(transaction))
        
    def delete(self, transaction = None):
        '''Delete the structure from the remote backend. If a transaction is
specified, the data is pipelined and executed when the transaction completes.'''
        cursor = self.cursor(transaction) if transaction else\
                     self.backend.cursor()
        return self._delete(cursor)
        
    def size(self, transaction = None):
        '''Number of elements in structure. If no transaction is
supplied, use the backend default cursor.'''
        if self._cache is None:
            if transaction:
                return self._size(self.cursor(transaction))
            else:
                return self._size(self.backend.cursor())
        else:
            return len(self._cache)
    
    def __contains__(self, value):
        return self.has(value)
    
    def __len__(self):
        return self.size()
    
    def has(self, value, transaction = None):
        '''Check if *value* is in the structure. Pass a transaction
if you would like ti pipeline the command.'''
        if self._cache is None:
            value = self.pickler.dumps(value)
            return self._unpicklefrom(self._has, transaction, False, None,
                                      value)
        else:
            return value in self._cache
        
    # INTERNAL METHODS
    
    def _pipe(self):
        self._setup()
        return self._pipe_
    
    def _cursor(self):
        self._setup()
        return self._transaction.cursor
    
    def _setup(self):
        # Build the pipeline for transaction if not already available.
        # The pipeline is obtained from a transaction in the backend server.
        # If an instance of a model is available, we build the transaction
        # from the local_transaction method.
        if not hasattr(self,'_pipe_'):
            if self.instance:
                transaction = self.instance.local_transaction(self.backend)
            else:
                transaction = self.backend.transaction()
            self._transaction = transaction
            self._pipe_ = self._transaction.structure_pipe(self)
            
    def _save_from_pipeline(self, cursor, pipeline):
        if pipeline:
            self._save(cursor, pipeline)
            pipeline.clear()
            if self.timeout:
                self.add_expiry()
        else:
            return 0
        
    def _unpicklefrom(self, func, transaction, default, loads,
                      *args, **kwargs):
        '''invoke remote function in the backend and if
we are not using a pipeline, unpickle the result.'''
        rk = ResultCallback(default,loads)
        if transaction:
            transaction.add(func, args, kwargs, callback = rk)
        else:
            return rk(func(self.backend.cursor(), *args, **kwargs))
            
    # PURE VIRTUAL METHODS
    
    def set_cache(self, r):
        # Called by the backend when preloading data
        raise NotimplementedError()
    
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
    def set_cache(self, r):
        loads = self.pickler.loads
        self._cache = list((loads(v) for v in r))
        
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
    
    def push_back(self, value, transaction = None):
        '''Appends a copy of *value* to the end of the remote list.'''
        self.pipe(transaction).push_back(self.pickler.dumps(value))
    
    def push_front(self, value, transaction = None):
        '''Appends a copy of *value* to the beginning of the remote list.'''
        self.pipe(transaction).push_front(self.pickler.dumps(value))


class Set(Structure):
    '''An unordered set :class:`stdnet.Structure`. Equivalent to python ``set``.
    
This structure is used for in two different parts of the library.

* It is the structure upon which indexes are built, therefore each :class:`stdnet.orm.Field`
  which has ``index`` set to ``True`` will have an associated
  Set structure in the data server backend.
* It is also used as :class:`stdnet.orm.SetField`.'''
    
    def set_cache(self, r):
        loads = self.pickler.loads
        self._cache = set((loads(v) for v in r))
                            
    def add(self, value, transaction = None):
        '''Add *value* to the set'''
        self.pipe(transaction).update((self.pickler.dumps(value),))

    def update(self, values, transaction = None):
        '''Add iterable *values* to the set'''
        d = self.pickler.dumps
        self.pipe(transaction).update((d(v) for v in values))
            
    def discard(self, value, transaction = None):
        '''Remove an element from a set if it is a member'''
        return self.remove((value,),transaction)
        
    def remove(self, values, transaction = None):
        '''Remove elements from a set if they are members'''
        dumps = self.pickler.dumps
        values = tuple((dumps(v) for v in values))
        return self._unpicklefrom(self._remove, transaction, None,
                                  self.pickler.loads, values)


class OrderedSet(Set):
    '''An ordered version of :class:`stdnet.Set`.'''
    def set_cache(self, r):
        loads = self.pickler.loads
        self._cache = list((loads(v) for v in r))
        
    def add(self, value, transaction = None):
        '''Add *value* to the set'''
        score = self.scorefun(value)
        self.pipe(transaction).add(score,self.pickler.dumps(value))

    def update(self, values, transaction = None):
        '''Add iterable *values* to the set'''
        s = self.scorefun
        d = self.pickler.dumps
        self.pipe(transaction).update(((s(v),d(v)) for v in values))
            
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
        
    def pop(self, key, *args, **kwargs):
        if args:
            if len(args) > 1:
                raise TypeError('pop expected at most 2 arguments, got {0}'\
                                .format(len(args)+1))
            default = args[0]
        else:
            default = KeyError(key)
        transaction = kwargs.get('transaction')
        key = self.pickler.dumps(key)
        return self._unpicklefrom(self._pop, transaction, default,
                                  self.value_pickler.loads, key)
        
    def add(self, key, value, transaction = None):
        '''Add ``key`` - ``value`` pair to hashtable.'''
        self.update({key:value},transaction)
    __setitem__ = add
    
    def addnx(self, field, value, transaction = None):
        '''Set the value of a hash field only if the field
does not exist.'''
        return self._addnx(self.cursor(transaction),
                           self.pickler.dumps(key),
                           self.pickler_value.dumps(value))
    
    def update(self, mapping, transaction = None):
        '''Add *mapping* dictionary to hashtable.
Equivalent to python dictionary update method.

:parameter mapping: a dictionary of field values,
:parameter transaction: a optional :class:`stadnet.Transaction` instance.'''
        tokey = self.pickler.dumps
        dumps = self.value_pickler.dumps
        pipe = self.pipe(transaction)
        if isinstance(mapping,dict):
            mapping = iteritems(mapping)
        pipe.update(dict(((tokey(k),dumps(v)) for k,v in mapping)))
    
    def get(self, key, default = None, transaction = None):
        '''Retrieve a single element from the hashtable.
If the element is not available return the default value (only
when not using a transaction).

:parameter key: lookup field
:parameter default: default value when the field is not available.
:parameter transaction: an optional transaction instance.
:rtype: a value in the hashtable or a pipeline depending if a
    transaction has been used.'''
        key = self.pickler.dumps(key)
        return self._unpicklefrom(self._get, transaction, default,
                                  self.value_pickler.loads, key)
    
    def __getitem__(self, key):
        v = self.get(key)
        if v is None:
            raise KeyError('%s not available' % key)
        else:
            return v
    
    def keys(self, desc = False):
        '''Return a generator of all keys. No transactions involved.'''
        kloads = self.pickler.loads
        for key in self._keys(self.backend.cursor()):
            yield kloads(key)

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
    
    def __iter__(self):
        # overrite the __iter__ method
        return self.keys()
    
    def sortedkeys(self, desc = True):
        keys = sorted(self.keys())
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
    
    def _keys(self, cursor):
        raise NotImplementedError
    
    def _items(self, cursor, keys):
        raise NotImplementedError
    
    def _pop(self, cursor, key):
        raise NotImplementedError

    
class TS(HashTable):
    '''A timeseries :class:`stdnet.Structure`. This is an experimental structure
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
        