'''Interfaces for supported data-structures'''

from stdnet.utils import iteritems

__all__ = ['PipeLine',
           'pipelines',
           'Structure',
           'List',
           'Set',
           'OrderedSet',
           'HashTable']

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


class keyconverter(object):
    
    @classmethod
    def tokey(cls, value):
        return value

    @classmethod
    def tovalue(cls, value):
        return value
    
    
class PipeLine(object):
    '''A pipeline utility class. Used to hold data in :class:`stdnet.Structure` before it is saved into the data server.'''
    def __init__(self, pipe, method, timeout):
        self.pipe = pipe
        self.method = method
        self.timeout = timeout
        
    def __repr__(self):
        return self.pipe.__repr__()
        
class HashPipe(PipeLine):
    def __init__(self, timeout):
        super(HashPipe,self).__init__({},'hash',timeout)
        
class TsPipe(PipeLine):
    def __init__(self, timeout):
        super(TsPipe,self).__init__({},'ts',timeout)

class SetPipe(PipeLine):
    def __init__(self, timeout):
        super(SetPipe,self).__init__(set(),'unordered_set',timeout)
        
class OsetPipe(PipeLine):
    def __init__(self, timeout):
        super(OsetPipe,self).__init__(set(),'ordered_set',timeout)        

class ListPipe(PipeLine):
    def __init__(self, timeout):
        super(ListPipe,self).__init__(listPipeline(),'list',timeout)
        

_pipelines = {'list':ListPipe,
              'hash': HashPipe,
              'ts': TsPipe,
              'set': SetPipe,
              'oset': OsetPipe}


def pipelines(typ, timeout):
    global _pipelines
    pip = _pipelines[typ]
    return pip(timeout)


class Structure(object):
    '''Base class for remote data-structures.
        
.. attribute:: cursor

    instance of a :class:`stdnet.BackendDataServer`.
    
.. attribute:: id

    unique *id* for the structure
    
.. attribute:: timeout

    Expiry timeout. If different from zero it represents the number of seconds
    after which the structure is deleted from the data server. Default ``0``.
    
.. attribute:: pipeline

    An instance of :class:`stdnet.PipeLine`.

    '''
    struct = None
    def __init__(self, cursor, id, timeout = 0,
                 pickler = None, pipeline = None, **kwargs):
        self.cursor    = cursor
        self.pickler   = pickler or cursor.pickler
        self.id        = id
        self.timeout   = timeout
        self._cache    = None
        if pipeline:
            self._pipeline = pipeline.pipe
            self.timeout   = pipeline.timeout
        else:
            self._pipeline = None
    
    def __repr__(self):
        base = '%s:%s' % (self.__class__.__name__,self.id)
        if self._cache is None:
            return base
        else:
            return '%s %s' % (base,self._cache)
        
    def __str__(self):
        return self.__repr__()
    
    def __get_pipeline(self):
        if self._pipeline is not None:
            return self._pipeline
        else:
            return self.cursor._get_pipe(self.id,self.struct,self.timeout).pipe
    pipeline = property(__get_pipeline)
        
    def size(self):
        '''Number of elements in structure'''
        if self._cache is None:
            return self._size()
        else:
            return len(self._cache)
    
    def __iter__(self):
        raise NotImplementedError()
    
    def _all(self):
        raise NotImplementedError
    
    def _size(self):
        raise NotImplementedError
    
    def delete(self):
        '''Delete structure from remote server.'''
        raise NotImplementedError
    
    def __len__(self):
        return self.size()
    
    def _unwind(self):
        if self._cache is None:
            self._cache = self._all()
        return self._cache
    
    def save(self):
        p = self.pipeline
        if p:
            s = self._save()
            if self.timeout:
                self.add_expiry()
            p.clear()
            return s
        else:
            return 0
        
    # PURE VIRTUAL METHODS
        
    def _save(self):
        raise NotImplementedError("Could not save")
    
    def add_expiry(self):
        '''Internal method called if a timeout is set. This needs to implemented.'''
        raise NotImplementedError("Could not save")


class List(Structure):
    '''A linked-list :class:`stdnet.Structure`.'''
    struct = ListPipe
    def __iter__(self):
        if not self._cache:
            cache = []
            loads = self.pickler.loads
            for item in self._all():
                item = loads(item)
                cache.append(item)
                yield item
            self.cache = cache
        else:
            for item in self.cache:
                yield item
    
    def pop_back(self):
        raise NotImplementedError
    
    def pop_front(self):
        raise NotImplementedError
    
    def push_back(self, value):
        '''Appends a copy of *value* to the end of the remote list.'''
        self.pipeline.push_back(self.pickler.dumps(value))
    
    def push_front(self, value):
        '''Appends a copy of *value* to the beginning of the remote list.'''
        self.pipeline.push_front(self.pickler.dumps(value))


class Set(Structure):
    '''An unordered set :class:`stdnet.Structure`. Equivalent to python ``set``.
    
This structure is used for in two different parts of the library.

* It is the structure upon which indexes are built, therefore each :class:`stdnet.orm.Field`
  which has ``index`` set to ``True`` will have an associated
  Set structure in the data server backend.
* It is also used as :class:`stdnet.orm.SetField`.'''
    struct = SetPipe
    def __iter__(self):
        if not self._cache:
            cache = []
            loads = self.pickler.loads
            for item in self._all():
                item = loads(item)
                cache.append(item)
                yield item
            self.cache = cache
        else:
            for item in self.cache:
                yield item
    
    def __contains__(self, value):
        value = self.pickler.dumps(value)
        if self._cache is None:
            return self._contains(value)
        else:
            return value in self._cache
                    
    def add(self, value):
        '''Add *value* to the set'''
        self.pipeline.add(self.pickler.dumps(value))

    def update(self, values):
        '''Add iterable *values* to the set'''
        pipeline = self.pipeline
        for value in values:
            pipeline.add(self.pickler.dumps(value))
            
    # PURE VIRTUAL METHODS
    
    def _contains(self, value):
        raise NotImplementedError
    
    def discard(self, elem):
        '''Remove an element from a set if it is a member'''
        raise NotImplementedError


class OrderedSet(Set):
    '''An ordered version of :class:`stdnet.Set`.'''
    struct = OsetPipe
    def __iter__(self):
        if not self._cache:
            cache = []
            loads = self.pickler.loads
            for item in self._all():
                item = loads(item)
                cache.append(item)
                yield item
            self.cache = cache
        else:
            for item in self.cache:
                yield item
                
    def add(self, value):
        '''Add *value* to the set'''
        self.pipeline.add((value.score(),self.pickler.dumps(value)))


class KeyValueStructure(Structure):
    '''Base class for :class:`HashTable`'''
    def __init__(self, *args, **kwargs):
        self.converter = kwargs.pop('converter',None) or keyconverter
        super(KeyValueStructure,self).__init__(*args, **kwargs)
    
    def __contains__(self, key):
        value = self.converter.tokey(key)
        if self._cache is None:
            return self._contains(value)
        else:
            return value in self._cache
    
    
    
class HashTable(Structure):
    '''A hash-table :class:`stdnet.Structure`.
The networked equivalent to a Python ``dict``.
    
This structure is important since it is used in two different parts of the library.

* It is the structure which holds instances for a :class:`stdnet.orm.StdModel` class.
  Therefore each model is represented as a HashTable structure.
  The keys are the model instances ids and the values are the 
  serialised version of the instances.

* It is also used as field (:class:`stdnet.orm.HashField`) like all other 
  class:`stdnet.Structure`.'''
    struct = HashPipe
    def __init__(self, *args, **kwargs):
        self.converter = kwargs.pop('converter',None) or keyconverter
        super(HashTable,self).__init__(*args, **kwargs)
        
    def __contains__(self, key):
        value = self.converter.tokey(key)
        if self._cache is None:
            return self._contains(value)
        else:
            return value in self._cache
        
    def add(self, key, value):
        '''Add ``key`` - ``value`` pair to hashtable.'''
        self.update({key:value})
    __setitem__ = add
    
    def update(self, mapping):
        '''Add *mapping* dictionary to hashtable. Equivalent to python dictionary update method.'''
        tokey = self.converter.tokey
        dumps = self.pickler.dumps
        p     = self.pipeline
        for key,value in iteritems(mapping):
            p[tokey(key)] = dumps(value)
    
    def get(self, key, default = None):
        kv = self.converter.tokey(key)
        value = self._get(kv)
        if value is not None:
            return self.pickler.loads(value)
        else:
            return default
    
    def __getitem__(self, key):
        v = self.get(key)
        if v is None:
            raise KeyError('%s not available' % key)
        else:
            return v
    
    def mget(self, keys):
        '''Return a generator of key-value pairs for the ``keys`` requested'''
        if not keys:
            raise StopIteration
        tokey = self.converter.tokey
        objs  = self._mget([tokey(key) for key in keys])
        loads = self.pickler.loads
        for obj in objs:
            yield loads(obj)
    
    def keys(self, desc = False):
        '''Return a generator of all keys.'''
        tovalue  = self.converter.tovalue
        for key in self._keys():
            yield tovalue(key)

    def items(self):
        '''Generator over all key-value items'''
        loads    = self.pickler.loads
        tovalue  = self.converter.tovalue
        for key,val in self._items():
            yield tovalue(key),loads(val)
            
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
    
    def clear(self):
        '''Clear the Hash table. Equivalent to ``dict.clear`` method in Python.'''
        raise NotImplementedError
    
    def _contains(self, value):
        raise NotImplementedError
    
    def _get(self, key):
        raise NotImplementedError
    
    def _keys(self):
        raise NotImplementedError
    
    def _items(self):
        raise NotImplementedError
    
    def _mget(self, keys):
        raise NotImplementedError

    
class TS(HashTable):
    struct = TsPipe

    def __init__(self, *args, **kwargs):
        super(TS,self).__init__(*args, **kwargs)
        
    def update(self, mapping):
        tokey = self.converter.tokey
        dumps = self.pickler.dumps
        p     = self.pipeline
        for key,value in mapping.iteritems():
            p[tokey(key)] = dumps(value)
    
    def front(self):
        try:
            return self.converter.tovalue(self._front())
        except:
            return None
        
    def back(self):
        try:
            return self.converter.tovalue(self._back())
        except:
            return None
        
    def range(self, start, end):
        '''Return a range between start and end key.'''
        tokey    = self.converter.tokey
        tovalue  = self.converter.tovalue
        loads    = self.pickler.loads
        for key,val in self._range(tokey(start),tokey(end)):
            yield tovalue(key),loads(val)
            
    def count(self, start, end):
        tokey    = self.converter.tokey
        return self._count(tokey(start),tokey(end))
            
    def irange(self, start = 0, end = -1):
        '''Return a range between start and end key.'''
        tovalue  = self.converter.tovalue
        loads    = self.pickler.loads
        for key,val in self._irange(start,end):
            yield tovalue(key),loads(val)
            
    def _count(self, start, end):
        raise NotImplementedError
    
    def _range(self, start, end):
        raise NotImplementedError
    
    def _irange(self, start, end):
        raise NotImplementedError
    
    def _front(self):
        raise NotImplementedError
    
    def _back(self):
        raise NotImplementedError
        