from inspect import isclass

from stdnet.conf import settings
from stdnet.utils import urlparse, encoders, itervalues, urlencode
from stdnet.utils.importer import import_module
from stdnet.exceptions import *

from .base import BackendDataServer


__all__ = ['getdb', 'getcache']


BACKENDS = {
    'redis': 'redisb',
    'dynamo': 'dynamob'
}


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
    
    
def getdb(backend=None, **kwargs):
    '''get a backend database'''
    if isinstance(backend, BackendDataServer):
        return backend
    backend = backend or settings.DEFAULT_BACKEND
    if not backend:
        return None
    scheme, address, params = parse_backend(backend)
    params.update(kwargs)
    backend = get_connection_string(scheme, address, params)
    params['connection_string'] = backend
    return _getdb(scheme, address, params)


def getcache(backend=None, encoder = encoders.PythonPickle, **kwargs):
    if isclass(encoder):
        encoder = encoder()
    db = getdb(backend=backend, pickler=encoder, **kwargs)
    return db.as_cache() 

        
