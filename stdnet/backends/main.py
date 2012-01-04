from inspect import isclass
from uuid import uuid4

from stdnet.conf import settings
from stdnet.utils import urlparse, encoders, itervalues, urlencode
from stdnet.utils.importer import import_module
from stdnet.lib.exceptions import ConnectionError 
from stdnet.exceptions import *

from .base import BackendDataServer


BACKENDS = {
    'redis': 'redisb',
}


def parse_backend_uri(backend_uri):
    """Converts the "backend_uri" into the database connection parameters.
It returns a (scheme, host, params) tuple."""
    r = urlparse.urlsplit(backend_uri)
    scheme, host = r.scheme, r.netloc
    if scheme not in ('https','http'):
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
    if scheme in BACKENDS:
        name = 'stdnet.backends.%s' % BACKENDS[scheme]
    else:
        name = scheme
    module = import_module(name)
    return getattr(module, 'BackendDataServer')(scheme, host, **params)


def get_connection_string(scheme, address, params):
    if isinstance(address,tuple):
        address = ':'.join(address)
    if params:
        address += '?' + urlencode(params)
    return scheme + '://' + address
    
    
def getdb(backend_uri = None, **kwargs):
    '''get a backend database'''
    if isinstance(backend_uri,BackendDataServer):
        return backend_uri
    backend_uri = backend_uri or settings.DEFAULT_BACKEND
    if not backend_uri:
        return None
    scheme, address, params = parse_backend_uri(backend_uri)
    params.update(kwargs)
    backend_uri = get_connection_string(scheme, address, params)
    params['connection_string'] = backend_uri
    return _getdb(scheme, address, params)


def getcache(backend_uri = None, encoder = encoders.PythonPickle, **kwargs):
    if isclass(encoder):
        encoder = encoder()
    return getdb(backend_uri = backend_uri, pickler = encoder, **kwargs) 


class CacheClass(object):
    '''Class which can be used as django cache backend'''
    
    def __init__(self, host, params):
        scheme = params.pop('type','redis')
        self.db = _getdb(scheme, host, params)
        self.timeout = self.db.default_timeout
        self.get     = self.db.get
        self.set     = self.db.set
        self.delete  = self.db.delete
        self.has_key = self.db.has_key
        self.clear   = self.db.clear
        

class make_struct(object):
    _structs = {}
    
    def __init__(self, name, pickler = None, value_pickler = None):
        self._name = name
        self._pickler = pickler
        self._value_pickler = value_pickler
    
    def __call__(self, server = None, id = None, pickler = None,
                 value_pickler = None, **kwargs):
        db = getdb(server)
        pickler = pickler or self._pickler
        value_pickler = value_pickler or self._value_pickler
        st = getattr(db,self._name)
        s = st(self._id(id),
               pickler = pickler or self._pickler,
               value_pickler = value_pickler or self._value_pickler,
               **kwargs)
        self._structs[s.id] = s
        return s
        
    def _id(self, id):
        return id or 'stdnet-{0}'.format(str(uuid4())[:8])
    
    @classmethod
    def clear(cls, ids):
        if ids:
            for id in ids:
                s = self._structs.pop(id,None)
                if s:
                    try:
                        s.delete()
                    except ConnectionError:
                        pass
        else:
            for s in itervalues(cls._structs):
                try:
                    s.delete()
                except ConnectionError:
                    pass
            cls._structs.clear()
            
    
class Structures(object):
    '''Create stdnet remote structures'''
    ids = set()
    
    list = make_struct('list')
    hash = make_struct('hash')
    dict = make_struct('hash', value_pickler = encoders.PythonPickle())
    set = make_struct('unordered_set')
    zset = make_struct('ordered_set')
    ts = make_struct('ts')
        
    def clear(self, *ids):
        make_struct.clear(ids)
    
    def all(self):
        return make_struct._structs
        
        
struct = Structures()