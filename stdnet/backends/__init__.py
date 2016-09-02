import sys
from collections import namedtuple
from inspect import isgenerator

try:
    from pulsar import maybe_async as async
except ImportError:     # pragma    noproxy

    def async(gen):
        raise NotImplementedError


from stdnet.utils.exceptions import *
from stdnet.utils import raise_error_trace
from stdnet.utils.importer import import_module
from stdnet.utils import (iteritems, int_or_float, to_string, urlencode,
                          urlparse)


__all__ = ['BackendStructure',
           'BackendDataServer',
           'BackendQuery',
           'session_result',
           'session_data',
           'instance_session_result',
           'query_result',
           'range_lookups',
           'getdb',
           'settings',
           'async']


query_result = namedtuple('query_result', 'key count')
# tuple containing information about a commit/delete operation on the backend
# server. Id is the id in the session, persistent is a boolean indicating
# if the instance is persistent on the backend, bid is the id in the backend.
instance_session_result = namedtuple('instance_session_result',
                                     'iid persistent id deleted score')
session_data = namedtuple('session_data',
                          'meta dirty deletes queries structures')
session_result = namedtuple('session_result', 'meta results')

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


class Settings(object):

    def __init__(self):
        self.DEFAULT_BACKEND = 'redis://127.0.0.1:6379?db=7'
        self.CHARSET = 'utf-8'
        self.REDIS_PY_PARSER = False
        self.ASYNC_BINDINGS = False


settings = Settings()


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


class BackendDataServer(object):

    '''Generic interface for a backend databases.

    It should not be initialised directly, the :func:`getdb` function should
    be used instead.

    :parameter name: name of database, such as **redis**, **mongo**, etc..
    :parameter address: network address of database server.
    :parameter charset: optional charset encoding. Default ``utf-8``.
    :parameter namespace: optional namespace for keys.
    :parameter params: dictionary of configuration parameters.

    **ATTRIBUTES**

    .. attribute:: name

        name of database

    .. attribute:: connection_string

        The connection string for this backend. By calling :func:`getdb`
        with this value, one obtain a :class:`BackendDataServer` connected to
        the same database as this instance.

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
                    related = dict(((obj.id, obj) for obj in
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

        :param instance: a :class:`stdnet.odm.Structure`
        :param client: Optional client handler.
        '''
        struct = self.struct_map.get(instance._meta.name)
        if struct is None:
            raise ModelNotAvailable('"%s" is not available for backend '
                                    '"%s"' % (instance._meta.name, self))
        client = client if client is not None else self.client
        return struct(instance, self, client)

    def execute(self, result, callback=None):
        if self.is_async():
            result = async(result)
            if callback:
                return result.add_callback(callback)
            else:
                return result
        else:
            if isgenerator(result):
                result = execute_generator(result)
            return callback(result) if callback else result

    # VIRTUAL METHODS
    def is_async(self):
        '''Check if the backend handler is asynchronous.'''
        return False

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
        return [self.basekey(obj._meta, obj.pkvalue())]

    def auto_id_to_python(self, value):
        '''Return a proper python value for the auto id.'''
        return value

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

    def flush(self, meta=None):
        '''Flush the database or drop all instances of a model/collection'''
        raise NotImplementedError()


class BackendQuery(object):

    '''Asynchronous query interface class.

    Implements the database queries specified by :class:`stdnet.odm.Query`.

    .. attribute:: queryelem

        The :class:`stdnet.odm.QueryElement` to process.

    .. attribute:: executed

        flag indicating if the query has been executed in the backend server

    '''

    def __init__(self, queryelem, timeout=0, **kwargs):
        '''Initialize the query for the backend database.'''
        self.queryelem = queryelem
        self.expire = max(timeout, 10)
        self.timeout = timeout
        self.__count = None
        self.__slice_cache = {}
        # build the queryset without performing any database communication
        self._build(**kwargs)

    def __repr__(self):
        return self.queryelem.__repr__()

    def __str__(self):
        return str(self.queryelem)

    @property
    def session(self):
        return self.queryelem.session

    @property
    def backend(self):
        return self.queryelem.backend

    @property
    def meta(self):
        return self.queryelem.meta

    @property
    def model(self):
        return self.queryelem.model

    @property
    def executed(self):
        return self.__count is not None

    @property
    def cache(self):
        '''Cached results.'''
        return self.__slice_cache

    def __len__(self):
        return self.execute_query()

    def count(self):
        return self.execute_query()

    def __contains__(self, val):
        self.execute_query()
        return self._has(val)

    def execute_query(self):
        if not self.executed:
            return self.backend.execute(self._execute_query(), self._got_count)
        return self.__count

    def __getitem__(self, slic):
        if isinstance(slic, slice):
            return self.items(slic)
        return self.backend.execute(self.items(), lambda r: r[slic])

    def items(self, slic=None, callback=None):
        return self.backend.execute(self._slice_items(slic), callback)

    def delete(self, qs):
        with self.session.begin() as t:
            t.delete(qs)
        return self.backend.execute(t.on_result,
                                    lambda _: t.deleted.get(self.meta))

    # VIRTUAL METHODS - MUST BE IMPLEMENTED BY BACKENDS

    def _has(self, val):    # pragma: no cover
        raise NotImplementedError

    def _items(self, slic):     # pragma: no cover
        raise NotImplementedError

    def _build(self, **kwargs):     # pragma: no cover
        raise NotImplementedError

    def _execute_query(self):       # pragma: no cover
        '''Execute the query without fetching data from server.

        Must be implemented by data-server backends and return a generator.
        '''
        raise NotImplementedError

    # PRIVATE METHODS

    def _got_count(self, c):
        self.__count = c
        return c

    def _slice_items(self, slic):
        key = None
        seq = self.__slice_cache.get(None)
        if slic:
            if seq is not None:  # we have the whole query cached already
                yield seq[slic]
            else:
                key = (slic.start, slic.step, slic.stop)
        if seq is not None:
            yield seq
        else:
            result = yield self.execute_query()
            items = ()
            if result:
                items = yield self._items(slic)
            session = self.session
            seq = []
            model = self.model
            for el in items:
                if isinstance(el, model):
                    session.add(el, modified=False)
                seq.append(el)
            self.__slice_cache[key] = seq
            yield seq


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
        raise NotImplementedError
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


def execute_generator(gen):
    exc_info = None
    result = None
    while True:
        try:
            if exc_info:
                result = failure.throw(*exc_info)
                exc_info = None
            else:
                result = gen.send(result)
        except StopIteration:
            break
        except Exception:
            if not exc_info:
                exc_info = sys.exc_info()
            else:
                break
        else:
            if isgenerator(result):
                result = execute_generator(result)
    #
    if exc_info:
        raise_error_trace(exc_info[1], exc_info[2])
    else:
        return result
