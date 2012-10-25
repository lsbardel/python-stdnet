import copy
import logging
from stdnet.utils import is_bytes_or_string
from stdnet.utils.importer import import_module
from stdnet import getdb, ModelNotRegistered

from .base import StdNetType, AlreadyRegistered
from .session import Manager, Session


logger = logging.getLogger('stdnet.mapper')


__all__ = ['clearall',
           'flush_models',
           'register',
           'unregister',
           'registered_models',
           'model_iterator',
           'all_models_sessions',
           'register_applications',
           'register_application_models']


def clearall(exclude=None):
    global _GLOBAL_REGISTRY
    exclude = exclude or []
    for model in _GLOBAL_REGISTRY:
        if not model._meta.name in exclude:
            model.objects.flush()
    Session.clearall()


def models_from_names(names):
    global _GLOBAL_REGISTRY
    s = set(names)
    for m in _GLOBAL_REGISTRY:
        if str(m._meta) in s:
            yield m


def flush_models(includes=None, excludes=None):
    '''Utility for flushing models data.
It removes all keys associated with models.'''
    global _GLOBAL_REGISTRY
    if includes:
        includes = list(models_from_names(includes))
    else:
        includes = _GLOBAL_REGISTRY
    if excludes:
        excludes = set(models_from_names(excludes))
    else:
        excludes = set()
    flushed = []
    for model in includes:
        if model not in excludes:
            model.objects.flush()
            flushed.append(str(model._meta))
    return flushed


def register(model, backend=None, ignore_duplicates=True,
             local_thread=False, **params):
    '''The low level function for registering a :class:`StdModel`
classes with a :class:`stdnet.BackendDataServer` data server.

:parameter model: a :class:`StdModel`. Must be provided.

:parameter backend: a backend connection string.
    For example::

        redis://localhost:8080?db=6&prefix=bla.

    Default ``settings.DEFAULT_BACKEND``.

:parameter params: optional parameters which can be used to override the
    connection string parameters.

**Usage**

For Redis the syntax is the following::

    import odm

    odm.register(Author, 'redis://my.host.name:6379/?db=1')
    odm.register(Book, 'redis://my.host.name:6379/?db=2')
    odm.register(MyOtherModel,
                'redis://my.host.name:6379/?db=2&keyprefix=differentprefix.')

``my.host.name`` can be ``localhost`` or an ip address or a domain name,
while ``db`` indicates the database number (very useful for separating data
on the same redis instance).'''
    if model in _GLOBAL_REGISTRY:
        if not ignore_duplicates:
            raise AlreadyRegistered(
                    'Model {0} is already registered'.format(model._meta))
        else:
            return
    backend = getdb(backend=backend, **params)
    for manager in model._managers:
        manager.backend = backend
    _GLOBAL_REGISTRY.add(model)
    return model.objects.backend


def unregister(model=None):
    '''Unregister a *model* if provided, otherwise it unregister all
registered models.'''
    global _GLOBAL_REGISTRY
    if model is not None:
        try:
            _GLOBAL_REGISTRY.remove(model)
        except KeyError:
            return 0
        for manager in model._managers:
            manager.backend = None
        return 1
    else:
        n = 0
        for model in list(_GLOBAL_REGISTRY):
            n += unregister(model)
        return n


def registered_models():
    '''An iterator over registered models'''
    return (m for m in _GLOBAL_REGISTRY)


def model_iterator(application):
    '''A generator of :class:`StdModel` classes found in *application*.

:parameter application: A python dotted path or an iterable over python
    dotted-paths where models are defined.

Only models defined in these paths are considered.

For example::

    from stdnet.odm import model_iterator

    APPS = ('stdnet.contrib.searchengine',
            'stdnet.contrib.timeseries')

    for model in model_iterator(APPS):
        ...

'''
    if not is_bytes_or_string(application):
        for app in application:
            for m in model_iterator(app):
                yield m
    else:
        label = application.split('.')[-1]
        try:
            mod = import_module(application)
        except ImportError:
            # the module is not there
            mod = None
        if mod:
            try:
                mod_models = import_module('.models', application)
            except ImportError:
                mod_models = mod
            mod_name = mod.__name__
            label = getattr(mod_models, 'app_label', label)
            for name in dir(mod_models):
                model = getattr(mod_models, name)
                meta = getattr(model, '_meta', None)
                if isinstance(model, StdNetType) and meta:
                    if not meta.abstract and meta.app_label == label:
                        yield model


def all_models_sessions(models, processed=None, session=None):
    '''Given an iterable over models, return a generator of the same models
plus hidden models such as the through model of :class:`ManyToManyField`
through models.'''
    processed = processed if processed is not None else set()
    for model in models:
        if model and model not in processed:
            try:
                model_session = model.objects.session()
            except ModelNotRegistered:
                model_session = session
            yield model, model_session
            processed.add(model)
            for field in model._meta.fields:
                if hasattr(field, 'relmodel'):
                    for m in all_models_sessions((field.relmodel,), processed):
                        yield m
                if hasattr(field, 'through'):
                    for m in all_models_sessions((field.through,), processed,
                                                 model_session):
                        yield m


def register_application_models(applications,
                                models=None,
                                app_defaults=None,
                                default=None):
    '''\
A higher level registration functions for group of models located
on application modules.
It uses the :func:`model_iterator` function to iterate
through all :class:`StdModel` models available in ``applications``
and register them using the :func:`register` low level function.

:parameter applications: A String or a list of strings which represent
    python dotted paths where models are implemented.
:parameter models: Optional list of models to include. If not provided
    all models found in *applications* will be included.
:parameter app_defaults: optional dictionary which specify a model and/or
    application backend connection string.
:parameter default: The default connection string.
:rtype: A generator over registered :class:`StdModel`.

For example::

    register_application_models('mylib.myapp')

'''
    app_defaults = app_defaults or {}
    for model in model_iterator(applications):
        meta = model._meta
        name = str(model._meta)
        if models and name not in models:
            continue
        if name not in app_defaults:
            name = model._meta.app_label
        kwargs = app_defaults.get(name, default)
        if not isinstance(kwargs, dict):
            kwargs = {'backend': kwargs}
        else:
            kwargs = kwargs.copy()
        if 'ignore_duplicates' not in kwargs:
            kwargs['ignore_duplicates'] = True
        if register(model, **kwargs):
            yield model


def register_applications(applications, **kwargs):
    '''A simple convenience wrapper around the
:func:`stdnet.odm.register_application_models` generator.

It return s a list of registered models.'''
    return list(register_application_models(applications, **kwargs))


_GLOBAL_REGISTRY = set()

