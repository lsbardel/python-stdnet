from stdnet.exceptions import *
from stdnet.utils import pickle, iteritems

from .structures import Structure

class default_pickler:
    
    @classmethod
    def loads(cls,x):
        return pickle.loads(x)
    
    @classmethod
    def dumps(cls, x):
        return pickle.dumps(x,2)


class NoPickle(object):
    
    def loads(self, s):
        return s
    
    def dumps(self, obj):
        return obj

nopickle = NoPickle()


class Keys(object):
    
    def __init__(self,id,timeout,pipeline):
        self.timeout = timeout
        self.value = None
        pipeline[id] = self
        
    def add(self, value):
        self.value = value
    

class BackendDataServer(object):
    '''Generic interface for a backend database:
    
    * *name* name of database, such as **redis**, **couchdb**, etc..
    * *params* dictionary of configuration parameters
    * *pickler* calss for serializing and unserializing data. It must implement the *loads* and *dumps* methods.
    '''
    structure_module = None
    def __init__(self, name, params, pickler = None):
        self.__name = name
        timeout = params.get('timeout', 0)
        try:
            timeout = int(timeout)
        except (ValueError, TypeError):
            timeout = 0
        self.default_timeout = timeout
        self._cachepipe = {}
        self._keys      = {}
        self.params     = params
        self.pickler    = pickler or default_pickler

    @property
    def name(self):
        return self.__name
    
    def __repr__(self):
        return '%s backend' % self.__name
    
    def __str__(self):
        return self.__repr__()
    
    def createdb(self, name):
        pass
    
    def isempty(self):
        '''Returns ``True`` if the database has no keys.'''
        keys = self.keys()
        if not hasattr(keys,'__len__'):
            keys = list(keys)
        return len(keys)
    
    def instance_keys(self, obj):
        raise NotImplementedError
    
    def delete(self, *key):
        "Delete one or more keys specified by ``keys``"
        raise NotImplementedError
    
    def _get_pipe(self, id, typ, timeout):
        cache  = self._cachepipe
        cvalue = cache.get(id,None)
        if cvalue is None:
            cvalue = typ(timeout)
            cache[id] = cvalue
        return cvalue
            
    def commit(self):
        '''Commit cache objects to database.'''
        cache = self._cachepipe
        keys = self._keys
        # flush cache
        self._cachepipe = {}
        self._keys = {}
        # commit
        for id,pipe in iteritems(cache):
            el = getattr(self,pipe.method)(id, pipeline = pipe)
            el.save()
        if keys: 
            self._set_keys(keys)
            
    def get_object(self, meta, name, value):
        '''Retrive an object from the database. If object is not available, it raises
an :class:`stdnet.exceptions.ObjectNotFound` exception.

    * *meta* :ref:`database metaclass <database-metaclass>` or model
    * *name* name of field (must be unique)
    * *value* value of field to search.'''
        raise NotImplementedError
    
    def save_object(self, obj, commit):
        '''Save an instance of a model to the back-end database:
        
        * *obj* instance of :ref:`StdModel <model-model>` to add to database
        * *commit* If True, *obj* is saved to database, otherwise it remains in local cache.
        '''
        raise NotImplementedError
    
    def save_object(self, obj, commit = True):
        '''Save a model object to the database:
        
        * *obj* instance of :ref:`StdModel <model-model>` to add to database
        * *commit* If True, *obj* is saved to database, otherwise it remains in local cache.
        '''
        raise NotImplementedError
    
    def delete_object(self, obj, deleted = None, multi_field = True):
        '''Delete an object from the data server and clean up indices.
Called to clear a model instance.
:parameter obj: instance of :class:`stdnet.orm.StdModel`
:parameter deleted: a list or ``None``. If a list, deleted keys will be appended to it.
:parameter multi_field: if ``True`` the multifield ids (if any) will be removed. Default ``True``.
        '''
        raise NotImplementedError
        
    def set(self, id, value, timeout = None):
        timeout = timeout if timeout is not None else self.default_timeout
        value = self.pickler.dumps(value)
        return self._set(id,value,timeout)
    
    def get(self, id, default = None):
        v = self._get(id)
        if v:
            return self.pickler.loads(v)
        else:
            return default

    def get_many(self, keys):
        """
        Fetch a bunch of keys from the cache. For certain backends (memcached,
        pgsql) this can be *much* faster when fetching multiple values.

        Returns a dict mapping each key in keys to its value. If the given
        key is missing, it will be missing from the response dict.
        """
        d = {}
        for k in keys:
            val = self.get(k)
            if val is not None:
                d[k] = val
        return d

    def has_key(self, key):
        """
        Returns True if the key is in the cache and has not expired.
        """
        return self.get(key) is not None

    def incr(self, key, delta=1):
        """
        Add delta to value in the cache. If the key does not exist, raise a
        ValueError exception.
        """
        if key not in self:
            raise ValueError("Key '%s' not found" % key)
        new_value = self.get(key) + delta
        self.set(key, new_value)
        return new_value

    def decr(self, key, delta=1):
        """
        Subtract delta from value in the cache. If the key does not exist, raise
        a ValueError exception.
        """
        return self.incr(key, -delta)

    def __contains__(self, key):
        """
        Returns True if the key is in the cache and has not expired.
        """
        # This is a separate method, rather than just a copy of has_key(),
        # so that it always has the same functionality as has_key(), even
        # if a subclass overrides it.
        return self.has_key(key)

    def delete_many(self, keys):
        """
        Set a bunch of values in the cache at once.  For certain backends
        (memcached), this is much more efficient than calling delete() multiple
        times.
        """
        for key in keys:
            self.delete(key)

    def clear(self):
        """Remove *all* values from the database at once."""
        raise NotImplementedError

    # VIRTUAL METHODS
    
    def keys(self, pattern = '*'):
        raise NotImplementedError
        
    def _set(self, id, value, timeout):
        raise NotImplementedError
    
    def _get(self, id):
        raise NotImplementedError
    
    def _set_keys(self):
        raise NotImplementedError
    
    def flush(self, meta, count = None):
        raise NotImplementedError
            
    # DATASTRUCTURES
    
    def index_keys(self, id, timeout):
        return Keys(id,timeout,self._keys)
    
    def list(self, id, timeout = 0, **kwargs):
        '''Return an instance of :class:`stdnet.List`
for a given *id*.'''
        return self.structure_module.List(self, id, timeout = timeout, **kwargs)
    
    def hash(self, id, timeout = 0, **kwargs):
        '''Return an instance of :class:`stdnet.HashTable` structure
for a given *id*.'''
        return self.structure_module.HashTable(self, id, timeout = timeout, **kwargs)
    
    def ts(self, id, timeout = 0, **kwargs):
        '''Return an instance of :class:`stdnet.HashTable` structure
for a given *id*.'''
        return self.structure_module.TS(self, id, timeout = timeout, **kwargs)
    
    def unordered_set(self, id, timeout = 0, **kwargs):
        '''Return an instance of :class:`stdnet.Set` structure
for a given *id*.'''
        return self.structure_module.Set(self, id, timeout = timeout, **kwargs)
    
    def ordered_set(self, id, timeout = 0, **kwargs):
        '''Return an instance of :class:`stdnet.OrderedSet` structure
for a given *id*.'''
        return self.structure_module.OrderedSet(self, id, timeout = timeout, **kwargs)
    

