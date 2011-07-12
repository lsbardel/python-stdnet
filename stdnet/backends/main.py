from inspect import isclass

from stdnet.conf import settings
from stdnet.utils import urlparse, encoders
from stdnet.utils.importer import import_module
from stdnet.exceptions import *

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


def getdb(backend_uri = None, **kwargs):
    backend_uri = backend_uri or settings.DEFAULT_BACKEND
    if not backend_uri:
        return None
    scheme, host, params = parse_backend_uri(backend_uri)
    params.update(kwargs)
    return _getdb(scheme, host, params)


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