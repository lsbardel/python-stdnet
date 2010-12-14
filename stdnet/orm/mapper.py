import copy
from stdnet import utils
from stdnet import getdb

from query import Manager, UnregisteredManager


__all__ = ['clearall',
           'register',
           'unregister',
           'Manager',
           'UnregisteredManager']


# lock used to synchronize the "mapper compile" step
_COMPILE_MUTEX = utils.threading.RLock()

    
def clearall(exclude = None):
    exclude = exclude or []
    for meta in _registry.values():
        if not meta.name in exclude:
            meta.cursor.clear()

def register(model, backend = None, keyprefix = None, timeout = 0):
    '''Register a :class:`stdnet.orm.StdModel` model with a :class:`stdnet.backends.BackendDataServer` data server.
    
    :keyword model: a :class:`stdnet.orm.StdModel` class. Must be provided.
    :keyword backend: a backend connection string. Default ``settings.DEFAULT_BACKEND``.
    :keyword keyprefix: a string used to prefix all database keys. Default ``settings.DEFAULT_KEYPREFIX``.
    :keyword keyprefix: timeout in seconds for keys persistance. Default ``0`` (no timeout).
    
    **Usage**
    
    For Redis the syntax is the following::
    
        import orm
        
        orm.register(Author, 'redis://my.host.name:6379/?db=1')
        orm.register(Book, 'redis://my.host.name:6379/?db=2')
        
    ``my.host.name`` can be ``localhost`` or an ip address or a domain name,
    while ``db`` indicates the database number (very useful for separating data
    on the same redis instance).'''
    global _registry
    from stdnet.conf import settings
    backend = backend or settings.DEFAULT_BACKEND
    prefix  = keyprefix or model._meta.keyprefix or settings.DEFAULT_KEYPREFIX or ''
    meta           = model._meta
    meta.keyprefix = prefix
    meta.timeout   = timeout or 0
    objects = getattr(model,'objects',None)
    if objects is None or isinstance(objects,UnregisteredManager):
        objects = Manager()
    else:
        objects = copy.copy(objects)
    model.objects    = objects
    meta.cursor      = getdb(backend)
    objects._setmodel(model)
    _registry[model] = meta
    return str(meta.cursor)


def unregister(model):
    global _registry 
    _registry.pop(model,None)
    model._meta.cursor = None

_registry = {}



class Mapper(object):
    
    def __init__(self,
                 class_,
                 local_table,
                 properties = None,
                 primary_key = None):
        self.class_ = class_
        self.local_table = local_table
        self.compiled = False
        
        _COMPILE_MUTEX.acquire()
        try:
            self._configure_inheritance()
            self._configure_extensions()
            self._configure_class_instrumentation()
            self._configure_properties()
            self._configure_pks()
            global _new_mappers
            _new_mappers = True
            self._log("constructed")
        finally:
            _COMPILE_MUTEX.release()
            