import json
from collections import namedtuple

from stdnet.conf import settings
from stdnet.utils.importer import import_module
from stdnet.exceptions import *
from stdnet.utils import zip, iteritems, itervalues, UnicodeMixin,\
                            int_or_float, to_string, urlencode, urlparse


__all__ = ['BackendRequest',
           'BackendStructure',
           'AsyncObject',
           'BackendDataServer',
           'BackendQuery',
           'CacheServer',
           'session_result',
           'instance_session_result',
           'query_result',
           'on_result',
           'range_lookups',
           'getdb',
           'getcache']


query_result = namedtuple('query_result','key count')
# tuple containing information about a commit/delete operation on the backend
# server. Id is the id in the session, persistent is a boolean indicating
# if the instance is persistent on the backend, bid is the id in the backend.
instance_session_result = namedtuple('instance_session_result',
                                     'iid persistent id deleted score')
session_result = namedtuple('session_result','meta results')

pass_through = lambda x: x
str_lower_case = lambda x: to_string(x).lower()

range_lookups = {
    'gt': int_or_float,
    'ge': int_or_float,
    'lt': int_or_float,
    'le': int_or_float,
    'contains': pass_through,
    'startswith': pass_through,
    'endswith': pass_through,
    'icontains': str_lower_case,
    'istartswith': str_lower_case,
    'iendswith': str_lower_case}
    

def get_connection_string(scheme, address, params):
    address = ':'.join((str(b) for b in address))
    if params:
        address += '?' + urlencode(params)
    return scheme + '://' + address


class BackendRequest(object):
    '''Signature class for Stdnet Request classes'''
    def add_callback(self, callback, errback=None):
        raise NotImplementedError()


def on_result(result, callback, *args, **kwargs):
    if isinstance(result, BackendRequest):
        return result.add_callback(lambda res : callback(res, *args, **kwargs))
    else:
        return callback(result, *args, **kwargs)
       

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
    
    
class BackendStructure(AsyncObject):
    __slots__ = ('instance', 'client', 'backend', '_id')
    
    def __init__(self, instance, backend, client):
        self.instance = instance
        self.backend = backend
        self.client = client
        if not instance.id:
            raise ValueError('No id available')
        # if structure has no instance create the database id
        if instance.instance is not None:
            id = instance.id
        else:
            id = backend.basekey(self.instance._meta, self.instance.id)
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
    def name(self):
        return self.instance.name
    
    @property
    def id(self):
        return self._id
    
    def backend_structure(self):
        return self
    
    def clone(self):
        return self.__class__(self.instance, self.client)
    
    def delete(self):   # pragma: no cover
        raise NotImplementedError()
    
    def flush(self):    # pragma: no cover
        raise NotImplementedError()
    
    def size(self):     # pragma: no cover
        raise NotImplementedError()
    
    
class BackendQuery(object):
    '''Backend queryset class which implements the database
queries specified by :class:`stdnet.odm.Query`.

.. attribute:: queryelem

    The :class:`stdnet.odm.QueryElement` to process.
    
.. attribute:: executed

    flag indicating if the query has been executed in the backend server
    
'''
    def __init__(self, queryelem, timeout=0, **kwargs):
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
        return on_result(self.execute_query(), self._get_items, slic)
    
    def execute_query(self):
        if not self.executed:
            return on_result(self._execute_query(), self._got_count)
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
    
    # PRIVATE
    def _got_count(self, c):
        self.__count = c
        return c
    
    def _get_items(self, c, slic):
        if c:
            return self._items(slic)
        else:
            return ()


class CacheServer(object):
    '''A key-value store server for storing and retrieving values at keys.'''
    def set(self, key, value, timeout=None):
        '''Set ``value`` at ``key`` with ``timeout``.'''
        raise NotImplementedError()
    
    def get(self, key, default=None):
        '''Fetch the value at ``key``.'''
        raise NotImplementedError()
    
    def __getitem__(self):
        v = self.get(key)
        if v is None:
            raise KeyError(key)
        else:
            return v
    
    def __setitem__(self, key, value):
        self.set(key, value)
    
    def __contains__(self, key):
        raise NotImplementedError()
    
    
class BackendDataServer(object):
    '''Generic interface for a backend database. It should not be initialised
directly, instead, the :func:`getdb` function should be used.
    
:parameter name: name of database, such as **redis**, **mongo**, etc..
:parameter address: network address of database server.
:parameter namespace: optional namespace for keys.
:parameter params: dictionary of configuration parameters.

**ATTRIBUTES**

.. attribute:: name

    name of database
    
.. attribute:: connection_string

    The connection string for this backend. By calling :func:`getdb` with this
    value, one obtain a :class:`BackendDataServer` connected to the
    same database as this instance.
    
.. attribute:: client

    The client handler for the backend database.
    
.. attribute:: Query

    The :class:`BackendQuery` class for this backend.
    
**METHODS**
'''
    Transaction = None
    Query = None
    structure_module = None
    default_port = 8000
    struct_map = {}
    async_handlers = {}
    
    def __init__(self, name=None, address=None, charset=None, namespace='',
                 **params):
        self.__name = name or 'dummy'
        address = address or ':'
        if not isinstance(address, (list, tuple)):
            address = address.split(':')
        else:
            address = list(address)
        if not address[0]:
            address[0] = '127.0.0.1'
        if len(address) == 2:
            if not address[1]:
                address[1] = self.default_port
            else:
                address[1] = int(address[1])
        self.charset = charset or 'utf-8'
        self.params = params
        self.namespace = namespace
        self.client = self.setup_connection(address)
        self.connection_string = get_connection_string(
                                        self.name, address, self.params)

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
        return self.client == other.client
    
    def basekey(self, meta, *args):
        """Calculate the key to access model data.
        
:parameter meta: a :class:`stdnet.odm.Metaclass`.
:parameter args: optional list of strings to prepend to the basekey.
:rtype: a native string
"""
        key = '%s%s' % (self.namespace, meta.modelkey)
        postfix = ':'.join((str(p) for p in args if p is not None))
        return '%s:%s' % (key, postfix) if postfix else key
    
    def disconnect(self):
        '''Disconnect the connection.'''
        pass
    
    def __repr__(self):
        return self.connection_string
    __str__ = __repr__
    
    def make_objects(self, meta, data, related_fields=None):
        '''Generator of :class:`stdnet.odm.StdModel` instances with data
from database.

:parameter meta: instance of model :class:`stdnet.odm.Metaclass`.
:parameter data: iterator over instances data.
'''
        make_object = meta.maker
        related_data = []
        if related_fields:
            for fname, fdata in iteritems(related_fields):
                field = meta.dfields[fname]
                if field in meta.multifields:
                    related = dict(fdata)
                    multi = True
                else:
                    multi = False
                    relmodel = field.relmodel
                    related = dict(((obj.id, obj)\
                        for obj in self.make_objects(relmodel._meta, fdata)))
                related_data.append((field, related, multi))
        for state in data:
            instance = make_object()
            instance.__setstate__(state)
            instance._dbdata[instance._meta.pkname()] = instance.pkvalue()
            for field, rdata, multi in related_data:
                if multi:
                    field.set_cache(instance, rdata.get(str(instance.id)))
                else:
                    rid = getattr(instance, field.attname, None)
                    if rid is not None:
                        value = rdata.get(rid)
                        setattr(instance, field.name, value)
            yield instance
    
    def objects_from_db(self, meta, data, related_fields=None):
        return list(self.make_objects(meta, data, related_fields))
            
    def structure(self, instance, client=None):
        '''Create a backend :class:`stdnet.odm.Structure` handler.
        
:parameter instance: a :class:`stdnet.odm.Structure`
:parameter client: Optional client handler'''
        struct = self.struct_map.get(instance._meta.name)
        if struct is None:
            raise ModelNotAvailable('structure "{0}" is not available for\
 backend "{1}"'.format(instance._meta.name,self))
        client = client if client is not None else self.client
        return struct(instance, self, client)
    
    def async(self):
        '''Returns an asynchronous :class:`BackendDataServer` with same
:attr:`connection_string`. Asynchronous hadlers must be implemented
by users.'''
        handler = self.async_handlers.get(self.name)
        if handler:
            return handler(self)
        else:
            raise NotImplementedError('Asynchronous handler for %s not '\
                                      'available.' % self)
        
    # VIRTUAL METHODS
    def setup_model(self, meta):
        '''Invoked when registering a model with a backend. This is a chance to
perform model specific operation in the server. For example, mongo db ensure
indices are created.'''
        pass
        
    def clean(self, meta):
        '''Remove temporary keys for a model'''
        pass
    
    def ping(self):
        '''Ping the server'''
        pass
    
    def instance_keys(self, obj):
        '''Return a list of database keys used by instance *obj*'''
        return [self.basekey(obj._meta, obj.id)]
    
    # PURE VIRTUAL METHODS
    
    def setup_connection(self, address):
        '''Callback during initialization. Implementation should override
this function for customizing their handling of connection parameters. It
must return a instance of the backend handler.'''
        raise NotImplementedError()
    
    def execute_session(self, session, callback):
        '''Execute a :class:`stdnet.odm.Session` in the backend server.'''
        raise NotImplementedError()
    
    def model_keys(self, meta):
        '''Return a list of database keys used by model *model*'''
        raise NotImplementedError()
        
    def as_cache(self):
        '''Return a :class:`CacheServer` handle for this backend.'''
        raise NotImplementedError('This backend cannot be used as cache')
    
    def flush(self, meta=None):
        '''Flush the database or drop all instances of a model/collection'''
        raise NotImplementedError()
    
    def publish(self, channel, message):
        '''Publish a *message* to a *channel*. The backend must support pub/sub
paradigm. For information check the
:ref:`Publish/Subscribe application <apps-pubsub>`.'''
        raise NotImplementedError('This backend cannot publish messages')
    
    def subscriber(self, **kwargs):
        '''Create a ``Subscriber`` able to subscribe to channels.
For information check the :ref:`Publish/Subscribe application <apps-pubsub>`.'''
        raise NotImplementedError()
    

def parse_backend(backend):
    """Converts the "backend" into the database connection parameters.
It returns a (scheme, host, params) tuple."""
    r = urlparse.urlsplit(backend)
    scheme, host = r.scheme, r.netloc
    if scheme not in ('https', 'http'):
        query = r.path
        path = ''
        if query:
            if query.find('?'):
                path = query
            else:
                query = query[1:]
    else:
        path, query = r.path, r.query
    
    if path:
        raise ImproperlyConfigured("Backend URI must not have a path.\
 Found {0}".format(path))
        
    if query:
        params = dict(urlparse.parse_qsl(query))
    else:
        params = {}

    return scheme, host, params


def _getdb(scheme, host, params):
    try:
        module = import_module('stdnet.backends.%sb' % scheme)
    except ImportError:
        module = import_module(scheme)
    return getattr(module, 'BackendDataServer')(scheme, host, **params)
    
    
def getdb(backend=None, **kwargs):
    '''get a :class:`BackendDataServer`.'''
    if isinstance(backend, BackendDataServer):
        return backend
    backend = backend or settings.DEFAULT_BACKEND
    if not backend:
        return None
    scheme, address, params = parse_backend(backend)
    params.update(kwargs)
    return _getdb(scheme, address, params)


def getcache(backend=None, **kwargs):
    '''Similar to :func:`getdb`, it creates a :class:`CacheServer`.'''
    db = getdb(backend=backend, **kwargs)
    return db.as_cache() 
