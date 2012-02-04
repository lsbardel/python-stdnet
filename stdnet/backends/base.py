import json
from collections import namedtuple

from stdnet.exceptions import *
from stdnet.utils import zip, iteritems, itervalues, encoders


__all__ = ['BackendDataServer', 'ServerOperation', 'BackendQuery',
           'session_result',
           'instance_session_result',
           'query_result']


query_result = namedtuple('query_result','key count')
# tuple containing information about a commit/delete operation on the backend
# server. Id is the id in the session, persistent is a boolean indicating
# if the instance is persistent on the backend, bid is the id in the backend.
instance_session_result = namedtuple('instance_session_result',
                                     'iid persistent id deleted')
session_result = namedtuple('session_result','meta results') 


def intid(id):
    try:
        return int(id)
    except ValueError:
        return id

        
class Keys(object):
    
    def __init__(self,id,timeout,pipeline):
        self.timeout = timeout
        self.value = None
        pipeline[id] = self
        
    def add(self, value):
        self.value = value
        

class ServerOperation(object):
    
    def __new__(cls, *args, **kwargs):
        o = super(ServerOperation,cls).__new__(cls)
        o.commands = None
        o._callbacks = []
        return o
    
    @property
    def done(self):
        return self.commands is not None
    
    
class BackendStructure(object):
    __slots__ = ('instance', 'client', '_id')
    def __init__(self, instance, backend, client):
        self.instance = instance
        self.client = client
        if not instance.id:
            raise ValueError('No id available')
        # if structure has no instance create the database id
        if instance.instance is not None:
            id = instance.id
        else:
            id = backend.basekey(self.instance._meta,self.instance.id)
        self._id = id
    
    def commit(self):
        '''Commit to backend server.'''
        instance = self.instance
        if instance.state().deleted:
            result = self.delete()
        else:
            result = self.flush()
        instance.cache.clear()
        return result
    
    @property
    def id(self):
        return self._id
    
    def backend_structure(self):
        return self
    
    def delete(self):
        raise NotImplementedError()
    
    def flush(self):
        raise NotImplementedError()
    
    def size(self):
        raise NotImplementedError()
    
    def clone(self):
        return self.__class__(self.instance,self.client)
    
    
class BackendQuery(ServerOperation):
    '''Backend queryset class which implements the database
queries specified by :class:`stdnet.orm.Query`.

.. attribute:: queryelem

    The :class:`stdnet.orm.QueryElement` to process.
    
.. attribute:: executed

    flag indicating if the query has been executed in the backend server
    
'''
    def __init__(self, queryelem, timeout = 0, **kwargs):
        '''Initialize the query for the backend database.'''
        self.queryelem = queryelem
        self.expire = max(timeout,10)
        self.timeout = timeout
        self.__count = None
        # build the queryset without performing any database communication
        self._build(**kwargs)

    def __repr__(self):
        return self.queryelem.__repr__()
    
    def __str__(self):
        return str(self.queryelem)
    
    @property
    def backend(self):
        return self.queryelem.backend
    
    @property
    def meta(self):
        return self.queryelem.meta
    
    @property
    def executed(self):
        return self.__count is not None
    
    def __len__(self):
        return self.execute_query()
    
    def count(self):
        return self.execute_query()

    def __contains__(self, val):
        self.execute_query()
        return self._has(val)
        
    def items(self, slic):
        if self.execute_query():
            return self._items(slic)
        else:
            return ()
    
    def execute_query(self):
        if self.__count is None:
            self.__count = self._execute_query()
        return self.__count
    
    # VIRTUAL FUNCTIONS
    
    def _has(self, val):
        raise NotImplementedError
    
    def _items(self, slic):
        raise NotImplementedError
    
    def _build(self, **kwargs):
        raise NotImplementedError
    
    def _execute_query(self):
        '''Execute the query without fetching data from server. Must
 be implemented by data-server backends.'''
        raise NotImplementedError
    

class BackendDataServer(object):
    '''\
Generic interface for a backend database:
    
:parameter name: name of database, such as **redis**, **couchdb**, etc..
:parameter params: dictionary of configuration parameters
:parameter pickler: calss for serializing and unserializing data.

It must implement the *loads* and *dumps* methods.'''
    Transaction = None
    Query = None
    structure_module = None
    
    def __init__(self, name, address, pickler = None,
                 charset = 'utf-8', connection_string = '',
                 **params):
        self.__name = name
        self._cachepipe = {}
        self._keys = {}
        self.charset = charset
        self.pickler = pickler or encoders.NoEncoder()
        self.connection_string = connection_string
        self.params = params
        self.client = self.setup_connection(address, **params)
        
    def setup_connection(self, address, **params):
        '''Callback during initialization. Implementation should override
this function for customizing their handling of connection parameters.'''
        raise NotImplementedError()
    
    def execute_session(self, session, callback):
        '''Execute a :class:`stdnet.orm.Session` in the backend server.'''
        raise NotImplementedError()

    @property
    def name(self):
        return self.__name
    
    def __ne__(self, other):
        return not self == other
    
    def __eq__(self, other):
        if self.__class__ == other.__class__:
            return self.issame(other)
        else:
            return False
        
    def issame(self, other):
        return False
    
    def cursor(self, pipelined = False):
        return self
    
    def disconnect(self):
        '''Disconnect the connection.'''
        pass
    
    def __repr__(self):
        return self.connection_string
    __str__ = __repr__
    
    def createdb(self, name):
        pass
    
    def isempty(self):
        '''Returns ``True`` if the database has no keys.'''
        keys = self.keys()
        if not hasattr(keys,'__len__'):
            keys = list(keys)
        return len(keys)
    
    def model_keys(self, meta):
        '''Return a list of database keys used by model *model*'''
        raise NotImplementedError()
        
    def instance_keys(self, obj):
        '''Return a list of database keys used by instance *obj*'''
        raise NotImplementedError()
    
    def execute_session(self, session):
        raise NotImplementedError()
    
    def structure(self, struct, clinet = None):
        raise NotImplementedError()
    
    def make_objects(self, meta, data, related_fields = None):
        '''Generator of :class:`stdnet.orm.StdModel` instances with data
from database.

:parameter meta: instance of model :class:`stdnet.orm.Metaclass`.
:parameter data: iterator over instances data.
'''
        make_object = meta.maker
        related_data = []
        if related_fields:
            for fname,fdata in iteritems(related_fields):
                field = meta.dfields[fname]
                if field in meta.multifields:
                    related = dict(fdata)
                    multi = True
                else:
                    multi = False
                    relmodel = field.relmodel
                    related = dict(((obj.id,obj)\
                            for obj in self.make_objects(relmodel._meta,fdata)))
                related_data.append((field,related,multi))
                
        for state in data:
            obj = make_object()
            obj.__setstate__(state)
            obj._dbdata['id'] = obj.id
            for field,rdata,multi in related_data:
                if multi:
                    field.set_cache(obj, rdata.get(str(obj.id)))
                else:
                    rid = getattr(obj,field.attname,None)
                    if rid is not None:
                        value = rdata.get(rid)
                        setattr(obj,field.name,value)
            yield obj
        
    def set(self, id, value, timeout = None):
        timeout = timeout or 0
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
        """Subtract delta from value in the cache.
If the key does not exist, raise a ValueError exception."""
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

    # PURE VIRTUAL METHODS
    
    def basekey(self, meta, *args):
        raise NotImplementedError()
    
    def clear(self):
        """Remove *all* values from the database at once."""
        raise NotImplementedError
    
    def keys(self, pattern = '*'):
        raise NotImplementedError
        
    def _set(self, id, value, timeout):
        raise NotImplementedError
    
    def _get(self, id):
        raise NotImplementedError
    
    def _set_keys(self):
        raise NotImplementedError
    
    def flush(self, meta):
        raise NotImplementedError
    
    def clean(self, meta):
        raise NotImplementedError
    

