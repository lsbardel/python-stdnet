'''Interfaces for supported data-structures'''
import stdnet
from stdnet.utils import iteritems, missing_intervals, encoders
from stdnet.lib import zset

__all__ = ['PipeLine',
           'Structure',
           'List',
           'Set',
           'OrderedSet',
           'HashTable',
           'TS']


default_score = lambda x : 1


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
 before it is saved into the data server.'''
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


class Structure(object):
    '''Base class for remote data-structures. Remote structures are the
backend of :ref:`structured fields <model-field-structure>` but they
can also be used as stand alone objects. For example::

    import stdnet
    db = stdnet.getdb(...)
    mylist = db.list('bla')
    
:parameter server: instance of the remote server where the structure is stored.
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
    If ``None`` the :attr:`server` pickler will be used.
    
    Default ``None``.
    
.. attribute:: scorefun

    A callable which takes a value as parameter and return a float
    number to be usedto score the value. Used in :class:`OrderedSet` structure.
    
    Default ``None``.

    '''
    def __init__(self, server, id,  pipetype, instance = None,
                 timeout = 0, pickler = None,
                 value_pickler = None, scorefun = None,
                 **kwargs):
        self.pipetype = pipetype
        self.server = server
        self.scorefun = scorefun
        self.instance = instance
        self.pickler = pickler if pickler is not None else server.pickler
        self.value_pickler = value_pickler if value_pickler is not None\
                                 else server.pickler
        self.id = id
        self._cache = None
        self.timeout = timeout

    @property
    def struct(self):
        return _pipelines[self.pipetype]
    
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
    
    def __iter__(self):
        if self._cache is None:
            cache = []
            loads = self.pickler.loads
            for item in self._all(self.server.cursor()):
                item = loads(item)
                cache.append(item)
                yield item
            self.cache = cache
        else:
            for item in self.cache:
                yield item

    def save(self, transaction = None):
        return self.save_from_pipeline(self.cursor(transaction),
                                       self.pipe(transaction))
        
    def size(self, transaction = None):
        '''Number of elements in structure. If no transaction is
supplied, use the server default cursor.'''
        if self._cache is None:
            if transaction:
                return self._size(self.cursor(transaction))
            else:
                return self._size(self.server.cursor())
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
        if not hasattr(self,'_pipe_'):
            if self.instance:
                transaction = self.instance.local_transaction()
            else:
                transaction = self.server.transaction()
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
        
    def _unpicklefrom(self, func, transaction, default, pickler,
                      *args, **kwargs):
        '''invoke remote function in the server and if
we are not using a pipeline, unpickle the result.'''
        if transaction:
            return func(self.cursor(transaction), *args, **kwargs)
        else:
            res = func(self.server.cursor(), *args, **kwargs)
            if res:
                if pickler:
                    return pickler.loads(res)
                else:
                    return res
            else:
                return default
        
    # PURE VIRTUAL METHODS
    
    def _has(self, cursor, value):
        raise NotImplementedError()
    
    def __iter__(self, cursor):
        raise NotImplementedError()
    
    def _all(self, cursor):
        raise NotImplementedError
    
    def _size(self, cursor):
        raise NotImplementedError
    
    def _remove(self, cursor, items):
        # Remove items form the structures
        raise NotImplementedError
    
    def _delete(self, cursor):
        '''Delete structure from remote server.'''
        raise NotImplementedError
    
    def _save(self, cursor, pipeline):
        raise NotImplementedError("Could not save")
    
    def add_expiry(self, cursor, timeout):
        '''Internal method called if a timeout is set.
This needs to implemented.'''
        raise NotImplementedError("Could not save")


class List(Structure):
    '''A linked-list :class:`stdnet.Structure`.'''
    def pop_back(self, transaction = None):
        return self._unpicklefrom(self._pop_back, transaction, None,
                                  self.pickler)
    
    def pop_front(self, transaction = None):
        return self._unpicklefrom(self._pop_front, transaction, None,
                                  self.pickler)
    
    def block_pop_back(self, timeout = None):
        return self._unpicklefrom(self._block_pop_back, transaction, None,
                                  self.pickler,
                                  timeout)
    
    def block_pop_front(self, timeout = None):
        return self._unpicklefrom(self._block_pop_front, transaction, None,
                                  self.pickler,
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
        return self._unpicklefrom(self._remove, transaction, None, self.pickler,
                                  values)


class OrderedSet(Set):
    '''An ordered version of :class:`stdnet.Set`.'''

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
    def __delitem__(self, key):
        self.pop(key)
        
    def pop(self, key, transaction = None, default = None):
        key = self.pickler.dumps(key)
        return self._unpicklefrom(self._pop, transaction, default,
                                  self.value_pickler, key)
        
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
        for key,value in iteritems(mapping):
            pipe[tokey(key)] = dumps(value)
    
    def get(self, key, default = None, transaction = None):
        '''Retrieve a single element from the hashtable.
If the element is not available return the default value (only
when not using a transaction).

:parameter key: lookup field
:parameter default: default value when the field is not available.
:parameter transaction: an optional transaction instance.
rtype: a value in the hashtable or a pipeline depending if a
    transaction has been used.'''
        key = self.pickler.dumps(key)
        return self._unpicklefrom(self._get, transaction, default,
                                  self.value_pickler, key)
    
    def __getitem__(self, key):
        v = self.get(key)
        if v is None:
            raise KeyError('%s not available' % key)
        else:
            return v
    
    def keys(self, desc = False):
        '''Return a generator of all keys. No transactions involved.'''
        kloads = self.pickler.loads
        for key in self._keys(self.server.cursor()):
            yield kloads(key)

    def items(self, keys = None):
        '''Generator over key-values.
If keys is not supplied, it is a generator over all key-value items.
No transaction involved in this function.'''
        kloads = self.pickler.loads
        vloads = self.value_pickler.loads
        if keys:
            dumps = self.pickler.dumps
            keys = [dumps(k) for k in keys]
            items = zip(keys,self._items(self.server.cursor(),keys))
        else:
            items = self._items(self.server.cursor(),keys)
        for key,val in items:
            yield kloads(key),vloads(val)
            
    def values(self, keys = None):
        '''Generator overvalues.
If keys is not supplied, it is a generator over value items.
No transaction involved in this function.'''
        kloads = self.pickler.loads
        vloads = self.value_pickler.loads
        if keys:
            dumps = self.pickler.dumps
            keys = [dumps(k) for k in keys]
            for item in self._items(self.server.cursor(),keys):
                yield vloads(item)
        else:
            for key,val in self._items(self.server.cursor(),keys):
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
    '''A timeseries :class:`stdnet.Structure`.
'''
    def front(self):
        '''Return the front key of timeseries'''
        try:
            return self.pickler.loads(self._front(self.server.cursor()))
        except TypeError:
            return None
        
    def back(self):
        '''Return the back key of timeseries'''
        try:
            return self.pickler.loads(self._back(self.server.cursor()))
        except TypeError:
            return None
        
    def range(self, start, end):
        '''Return a generator of a range between start and end key.'''
        tokey = self.pickler.dumps
        kloads = self.pickler.loads
        vloads = self.value_pickler.loads
        cursor = self.server.cursor()
        for key,val in self._range(cursor,tokey(start),tokey(end)):
            yield kloads(key),vloads(val)
            
    def count(self, start, end):
        tokey    = self.pickler.dumps
        return self._count(self.server.cursor(),
                           tokey(start),tokey(end))
            
    def irange(self, start = 0, end = -1):
        '''Return a range between start and end key.'''
        kloads = self.pickler.loads
        vloads = self.value_pickler.loads
        for key,val in self._irange(self.server.cursor(),start,end):
            yield kloads(key),vloads(val)
            
    def intervals(self, startdate, enddate, parseinterval = None,
                    dateconverter = None):
        start = self.front()
        end = self.back()
        return missing_intervals(startdate, enddate, start, end,
                                 parseinterval = parseinterval,
                                 dateconverter = dateconverter)
            
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
        