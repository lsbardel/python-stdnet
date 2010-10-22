from cgi import parse_qsl

from stdnet.conf import settings
from stdnet.utils import import_module
from stdnet.exceptions import *


BACKENDS = {
    'memcached': 'memcached',
    'locmem': 'locmem',
    'file': 'filebased',
    'db': 'db',
    'dummy': 'dummy',
    'redis': 'redisb',
    'couchdb': 'couchdbb'
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


def getdb(backend_uri = None, pickler = None):
    backend_uri = backend_uri or settings.DEFAULT_BACKEND
    if not backend_uri:
        return None
    scheme, host, params = parse_backend_uri(backend_uri)
    if scheme in BACKENDS:
        name = 'stdnet.backends.%s' % BACKENDS[scheme]
    else:
        name = scheme
    module = import_module(name)
    return getattr(module, 'BackendDataServer')(scheme, host, params, pickler = pickler)

