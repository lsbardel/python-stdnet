import copy
from stdnet import getdb

from query import Manager, UnregisteredManager
    
def clearall():
    for meta in _registry.values():
        meta.cursor.clear()

def register(model, backend = None, keyprefix = None, timeout = 0):
    '''Register a :class:`stdnet.rom.StdNet` model with a backend data server.'''
    global _registry
    from stdnet.conf import settings
    backend = backend or settings.DEFAULT_BACKEND
    prefix  = keyprefix or model._meta.keyprefix or settings.DEFAULT_KEYPREFIX or ''
    if prefix:
        prefix = '%s:' % prefix
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
    objects.model    = model
    objects._meta    = meta
    objects.cursor   = meta.cursor
    _registry[model] = meta
    return meta.cursor.name


def unregister(model):
    global _registry 
    _registry.pop(model,None)
    model._meta.cursor = None

_registry = {}
