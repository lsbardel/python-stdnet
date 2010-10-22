'''Interfaces for supported data-structures'''

from stdnet.utils import listPipeline, many2manyPipeline


__all__ = ['PipeLine',
           'pipelines',
           'Structure',
           'List',
           'Set',
           'OrderedSet',
           'HashTable']

class keyconverter(object):
    
    @classmethod
    def tokey(cls, value):
        return value

    @classmethod
    def tovalue(cls, value):
        return value
    
    
class PipeLine(object):
    '''Struicture pipeline'''
    def __init__(self, pipe, method, timeout):
        self.pipe = pipe
        self.method = method
        self.timeout = timeout
        
    def __repr__(self):
        return self.pipe.__repr__()
        
class HashPipe(PipeLine):
    def __init__(self, timeout):
        super(HashPipe,self).__init__({},'hash',timeout)

class SetPipe(PipeLine):
    def __init__(self, timeout):
        super(SetPipe,self).__init__(set(),'unordered_set',timeout)
        
class OsetPipe(PipeLine):
    def __init__(self, timeout):
        super(OsetPipe,self).__init__(set(),'ordered_set',timeout)        

class ListPipe(PipeLine):
    def __init__(self, timeout):
        super(ListPipe,self).__init__(listPipeline(),'list',timeout)
        
class Many2Many(PipeLine):
    def __init__(self, timeout):
        super(Many2Many,self).__init__(many2manyPipeline(),'unordered_set',timeout)

_pipelines = {'list':ListPipe,
              'hash': HashPipe,
              'set': SetPipe,
              'oset': OsetPipe,
              'many2many': Many2Many}


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
    
.. attribute:: _pipeline

    An instance :class:`PipeLine`

    '''
    
    def __init__(self, cursor, id, pipeline = None,
                 pickler = None, **kwargs):
        self.cursor    = cursor
        self.pickler   = pickler or cursor.pickler
        self.id        = id
        self._pipeline = pipeline
        self._cache    = None
    
    def __repr__(self):
        base = '%s:%s' % (self.__class__.__name__,self.id)
        if self._cache is None:
            return base
        else:
            return '%s %s' % (base,self._cache)
        
    def __str__(self):
        return self.__repr__()
    
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
        if self._pipeline:
            s = self._save()
            self._pipeline.clear()
            return s
        else:
            return 0
        
    # PURE VIRTUAL METHODS
        
    def _save(self):
        raise NotImplementedError("Could not save")


class List(Structure):
    '''A linked-list :class:`stdnet.Structure`.'''
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
        self._pipeline.push_back(self.pickler.dumps(value))
    
    def push_front(self, value):
        '''Appends a copy of *value* to the beginning of the remote list.'''
        self._pipeline.push_front(self.pickler.dumps(value))



class Set(Structure):
    '''An unordered set :class:`stdnet.Structure`.'''
    
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
        self._pipeline.add(self.pickler.dumps(value))

    def update(self, values):
        '''Add iterable *values* to the set'''
        pipeline = self._pipeline
        for value in values:
            pipeline.add(self.pickler.dumps(value))
            
    # PURE VIRTUAL METHODS
    
    def _contains(self, value):
        raise NotImplementedError
    
    def discard(self, elem):
        '''Remove an element from a set if it is a member'''
        raise NotImplementedError


class OrderedSet(Set):
    '''An ordered set :class:`stdnet.Structure`.'''
    
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
        self._pipeline.add((value.score(),self.pickler.dumps(value)))


def itemcmp(x,y):
    if x[0] > y[0]:
        return 1
    else:
        return -1

    
class HashTable(Structure):
    '''An hash-table :class:`stdnet.Structure`.'''
    
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
        '''Add *key* - *value* pair to hashtable.'''
        self.update({key:value})
    __setitem__ = add
    
    def update(self, mapping):
        '''Add *mapping* dictionary to hashtable. Equivalent to python dictionary update method.'''
        tokey = self.converter.tokey
        dumps = self.pickler.dumps
        p     = self._pipeline
        for key,value in mapping.iteritems():
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
        '''Return a generator of key-value pairs for the keys requested'''
        if not keys:
            raise StopIteration
        tokey = self.converter.tokey
        ckeys = [tokey(key) for key in keys]
        objs  = self._mget(ckeys)
        loads = self.pickler.loads
        for obj in objs:
            yield loads(obj)
    
    def keys(self, desc = False):
        tovalue  = self.converter.tovalue
        for key in self._keys():
            yield tovalue(key)

    def items(self):
        '''Generator over key-value items'''
        result   = self._items()
        loads    = self.pickler.loads
        tovalue  = self.converter.tovalue
        for key,val in result.iteritems():
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
        items = sorted(self.items(),cmp = itemcmp)
        if not desc:
            items = reversed(items)
        return items
    
    # PURE VIRTUAL METHODS
    
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

    
