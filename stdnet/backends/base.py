import json
from copy import deepcopy

from stdnet.exceptions import *
from stdnet.utils import zip, iteritems, itervalues, encoders


__all__ = ['BackendDataServer', 'BackendQuery']


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
    
        
class BackendQuery(object):
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
        self.expire = max(timeout,30)
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
    
    @property
    def query_class(self):
        '''The underlying query class'''
        return self.queryelem.__class__
    
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
    
    def delete(self, related_queries):
        raise NotImplementedError()
    
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
    
    # LOAD RELATED
    
    def load_related(self, result):
        '''load related fields into the query result.

:parameter result: a result from a queryset.
:rtype: the same queryset qith related models loaded.'''
        if self.queryelem.select_related:
            if not hasattr(result,'__len__'):
                result = list(result)
            meta = self.meta
            for field in self.queryelem.select_related:
                name = field.name
                attname = field.attname
                vals = [getattr(r,attname) for r in result]
                if field in meta.scalarfields:
                    related = field.relmodel.objects.filter(id__in = vals)
                    for r,val in zip(result,related):
                        setattr(r,name,val)
                else:
                    with self.backend.transaction() as t:
                        for val in vals:
                            val.reload(t)
                    for val,r in zip(vals,t.get_result()):
                        val.set_cache(r)
                        
        return result
    

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
    
    def model_keys(self, model):
        '''Return a list of database keys used by model *model*'''
        raise NotImplementedError()
        
    def instance_keys(self, obj):
        '''Return a list of database keys used by instance *obj*'''
        raise NotImplementedError()
    
    def execute_session(self, session):
        raise NotImplementedError()
    
    def structure(self, struct):
        raise NotImplementedError()
    
    def make_objects(self, meta, data):
        '''Generator of :class:`stdnet.orm.StdModel` instances with data
from database.

:parameter meta: instance of model :class:`stdnet.orm.Metaclass`.
:parameter data: iterator over instances data.
'''
        make_object = meta.maker
        for state in data:
            obj = make_object()
            obj.__setstate__(state)
            #fields['__dbdata__'] = deepcopy(fields)
            obj._dbdata['id'] = obj.id
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
    

