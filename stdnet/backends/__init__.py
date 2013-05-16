from collections import namedtuple

from stdnet.utils.exceptions import *
from stdnet.utils.conf import settings
from stdnet.utils.importer import import_module
from stdnet.utils import iteritems, int_or_float, to_string, urlencode, urlparse


__all__ = ['BackendStructure',
           'BackendDataServer',
           'CacheServer',
           'session_result',
           'session_data',
           'instance_session_result',
           'query_result',
           'range_lookups',
           'getdb',
           'getcache']


query_result = namedtuple('query_result','key count')
# tuple containing information about a commit/delete operation on the backend
# server. Id is the id in the session, persistent is a boolean indicating
# if the instance is persistent on the backend, bid is the id in the backend.
instance_session_result = namedtuple('instance_session_result',
                                     'iid persistent id deleted score')
session_data = namedtuple('session_data', 'meta dirty deletes queries structures')
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
    if address:
        address = ':'.join((str(b) for b in address))
    else:
        address = ''
    if params:
        address += '?' + urlencode(params)
    return scheme + '://' + address

    
class BackendStructure(object):
    '''Interface for :class:`stdnet.odm.Structure` backends.
    
.. attribute:: instance

    The :class:`stdnet.odm.Structure` which this backend represents.
    
.. attribute:: backend

    The :class:`BackendDataServer`
    
.. attribute:: client

    The client of the :class:`BackendDataServer`
    
'''
    def __init__(self, instance, backend, client):
        self.instance = instance
        self.backend = backend
        self.client = client
    
    @property
    def name(self):
        return self.instance.name
    
    def backend_structure(self):
        return self
    
    def clone(self):
        return self.__class__(self.instance, self.backend, self.client)
    
    def delete(self):
        raise NotImplementedError
    
    def flush(self):
        raise NotImplementedError
    
    def size(self):
        raise NotImplementedError
    
    
class CacheServer(object):
    '''A key-value store server for storing and retrieving values at keys.'''
    def set(self, key, value, timeout=None):
        '''Set ``value`` at ``key`` with ``timeout``.'''
        raise NotImplementedError
    
    def get(self, key, default=None):
        '''Fetch the value at ``key``.'''
        raise NotImplementedError
    
    def __getitem__(self):
        v = self.get(key)
        if v is None:
            raise KeyError(key)
        else:
            return v
    
    def __setitem__(self, key, value):
        self.set(key, value)
    
    def __contains__(self, key):
        raise NotImplementedError
    
    
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
    
.. attribute:: default_manager

    The default model Manager for this backend. If not
    provided, the :class:`stdnet.odm.Manager` is used.
    Default ``None``.
    
'''
    Query = None
    structure_module = None
    default_manager = None
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
    
    def __hash__(self):
        return id(self)
            
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
        make_object = meta.make_object
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
                    related = dict(((obj.id, obj) for obj in\
                            self.make_objects(relmodel._meta, fdata)))
                related_data.append((field, related, multi))
        for state in data:
            instance = make_object(state, self)
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
    
    def auto_id_to_python(self, value):
        '''Return a proper python value for the auto id.'''
        return value
    
    def bind_before_send(self, callback):
        pass
    
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
    path, query = r.path, r.query
    if path and not query:
        query, path = path, ''
        if query:
            if query.find('?'):
                path = query
            else:
                query = query[1:]
    if query:
        params = dict(urlparse.parse_qsl(query))
    else:
        params = {}

    return scheme, host, params


def _getdb(scheme, host, params):
    try:
        module = import_module('stdnet.backends.%sb' % scheme)
    except ImportError:
        module = import_module('stdnet.backends.sql')
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
    if 'timeout' in params:
        params['timeout'] = int(params['timeout'])
    return _getdb(scheme, address, params)


def getcache(backend=None, **kwargs):
    '''Similar to :func:`getdb`, it creates a :class:`CacheServer`.'''
    db = getdb(backend=backend, **kwargs)
    return db.as_cache() 
