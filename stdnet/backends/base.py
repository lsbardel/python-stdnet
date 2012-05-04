import json
from collections import namedtuple

from stdnet.conf import settings
from stdnet.exceptions import *
from stdnet.utils import zip, iteritems, itervalues, encoders, UnicodeMixin


__all__ = ['BackendRequest',
           'AsyncObject',
           'BackendDataServer',
           'ServerOperation',
           'BackendQuery',
           'session_result',
           'instance_session_result',
           'query_result']


query_result = namedtuple('query_result','key count')
# tuple containing information about a commit/delete operation on the backend
# server. Id is the id in the session, persistent is a boolean indicating
# if the instance is persistent on the backend, bid is the id in the backend.
instance_session_result = namedtuple('instance_session_result',
                                     'iid persistent id deleted score')
session_result = namedtuple('session_result','meta results') 


class BackendRequest(object):
    '''Signature class for Stdnet Request classes'''
    pass


class AsyncObject(UnicodeMixin):
    '''A class for handling asynchronous requests. The main method here
is :meth:`async_handle`. Avery time there is a result from the server,
this method should be called.'''
    def async_handle(self, result, callback, *args, **kwargs):
        if isinstance(result, BackendRequest):
            return result.add_callback(lambda res :\
                        self.async_callback(callback, res, *args, **kwargs))
        else:
            return self.async_callback(callback, result, *args, **kwargs)
        
    def async_callback(self, callback, result, *args, **kwargs):
        if isinstance(result, Exception):
            raise result
        else:
            return callback(result, *args, **kwargs)


class ServerOperation(object):
    
    def __new__(cls, *args, **kwargs):
        o = super(ServerOperation,cls).__new__(cls)
        o.commands = None
        o._callbacks = []
        return o
    
    @property
    def done(self):
        return self.commands is not None
    
    
class BackendStructure(AsyncObject):
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
    
    def clone(self):
        return self.__class__(self.instance,self.client)
    
    def delete(self):   # pragma: no cover
        raise NotImplementedError()
    
    def flush(self):    # pragma: no cover
        raise NotImplementedError()
    
    def size(self):     # pragma: no cover
        raise NotImplementedError()
    
    
class BackendQuery(ServerOperation):
    '''Backend queryset class which implements the database
queries specified by :class:`stdnet.odm.Query`.

.. attribute:: queryelem

    The :class:`stdnet.odm.QueryElement` to process.
    
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
    
    def _has(self, val):    # pragma: no cover
        raise NotImplementedError()
    
    def _items(self, slic):     # pragma: no cover
        raise NotImplementedError()
    
    def _build(self, **kwargs):     # pragma: no cover
        raise NotImplementedError()
    
    def _execute_query(self):       # pragma: no cover
        '''Execute the query without fetching data from server. Must
 be implemented by data-server backends.'''
        raise NotImplementedError()
    

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
    struct_map = {}
    
    def __init__(self, name, address, pickler = None,
                 charset = 'utf-8', connection_string = '',
                 prefix = None, **params):
        self.__name = name
        self._cachepipe = {}
        self._keys = {}
        self.charset = charset
        self.pickler = pickler or encoders.NoEncoder()
        self.connection_string = connection_string
        self.params = params
        self.namespace = prefix if prefix is not None else\
                         settings.DEFAULT_KEYPREFIX
        self.client = self.setup_connection(address, **params)

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
    
    def isempty(self):
        '''Returns ``True`` if the database has no keys.'''
        keys = self.keys()
        if not hasattr(keys,'__len__'):
            keys = list(keys)
        return len(keys)
    
    def make_objects(self, meta, data, related_fields = None):
        '''Generator of :class:`stdnet.odm.StdModel` instances with data
from database.

:parameter meta: instance of model :class:`stdnet.odm.Metaclass`.
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
            
    def structure(self, instance, client = None):
        '''Create a backend :class:`stdnet.odm.Structure` handler.'''
        struct = self.struct_map.get(instance._meta.name)
        if struct is None:
            raise ModelNotAvailable('structure "{0}" is not available for\
 backend "{1}"'.format(instance._meta.name,self))
        client = client if client is not None else self.client
        return struct(instance, self, client)
        
    def clean(self, meta):
        '''Remove temporary keys for a model'''
        pass
    
    def basekey(self, meta, *args):
        """Calculate the key to access model data.
        
:parameter meta: a :class:`stdnet.odm.Metaclass`.
:parameter args: optional list of strings which are attached to the basekey.
:rtype: a native string
"""
        key = '{0}{1}'.format(self.namespace,meta.modelkey)
        postfix = ':'.join((str(p) for p in args if p is not None))
        return '{0}:{1}'.format(key,postfix) if postfix else key
    
    # PURE VIRTUAL METHODS
    
    def setup_connection(self, address, **params):  # pragma: no cover
        '''Callback during initialization. Implementation should override
this function for customizing their handling of connection parameters. It
must return a instance of the backend handler.'''
        raise NotImplementedError()
    
    def execute_session(self, session, callback):   # pragma: no cover
        '''Execute a :class:`stdnet.odm.Session` in the backend server.'''
        raise NotImplementedError()
    
    def model_keys(self, meta):     # pragma: no cover
        '''Return a list of database keys used by model *model*'''
        raise NotImplementedError()
        
    def instance_keys(self, obj):   # pragma: no cover
        '''Return a list of database keys used by instance *obj*'''
        raise NotImplementedError()
    
    def as_cache(self):     # pragma: no cover
        raise NotImplementedError('This backend cannot be used as cache')
    
    def clear(self):    # pragma: no cover
        """Remove *all* values from the database at once."""
        raise NotImplementedError()
    
    def flush(self, meta = None, pattern = None):   # pragma: no cover
        raise NotImplementedError()
    
    def subscriber(self):   # pragma: no cover
        raise NotImplementedError()
    

