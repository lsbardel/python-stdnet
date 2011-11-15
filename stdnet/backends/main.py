from inspect import isclass
from uuid import uuid4

from stdnet.conf import settings
from stdnet.utils import urlparse, encoders, itervalues, urlencode
from stdnet.utils.importer import import_module
from stdnet.lib.exceptions import ConnectionError 
from stdnet.exceptions import *

from .base import BackendDataServer


parse_qsl = urlparse.parse_qsl


BACKENDS = {
    'redis': 'redisb',
}


def parse_backend_uri(backend_uri):
    """Form django source code.
    Converts the "backend_uri" into a cache scheme ('db', 'memcached', etc), a
    host and any extra params that are required for the backend. Returns a
    (scheme, host, params) tuple.
    """
    if backend_uri.find(':') == -1:
        raise ImproperlyConfigured("Backend URI must start with scheme://")
    scheme, rest = backend_uri.split(':', 1)
    if not rest.startswith('//'):
        raise ImproperlyConfigured("Backend URI must start with scheme://")
    host = rest[2:]
    qpos = rest.find('?')
    if qpos != -1:
        params = dict(parse_qsl(rest[qpos+1:]))
        host = rest[2:qpos]
    else:
        params = {}
    if host.endswith('/'):
        host = host[:-1]

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
    
    def __init__(self, name):
        self._name = name
    
    def __call__(self, server = None, id = None, **kwargs):
        db = getdb(server)
        s = getattr(db,self._name)(self._id(id),**kwargs)
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
    set = make_struct('unordered_set')
    zset = make_struct('ordered_set')
    ts = make_struct('ts')
        
    def clear(self, *ids):
        make_struct.clear(ids)
    
    def all(self):
        return make_struct._structs
        
    
    
struct = Structures()