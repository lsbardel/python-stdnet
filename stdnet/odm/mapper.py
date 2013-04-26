import copy
import logging
from inspect import ismodule

from stdnet.utils import native_str
from stdnet.lib import multi_async
from stdnet.utils.importer import import_module
from stdnet import getdb, ModelNotRegistered

from .base import StdNetType, AlreadyRegistered
from .session import Manager, Session


logger = logging.getLogger('stdnet.mapper')


__all__ = ['Router',
           'flush_models',
           'register',
           'unregister',
           'registered_models',
           'model_iterator',
           'all_models_sessions',
           'register_applications']
        
        
class Router(object):
    '''A router of models to their manager::
    
    mapper = Router()
    mapper.register(MyModel, ...)
    
    query = mapper[MyModel].query()
    '''
    def __init__(self, default_backend=None, install_global=False):
        self._registered_models = {}
        self._default_backend = getdb(default_backend)
        self._install_global = install_global
        
    @property
    def default_backend(self):
        return self._default_backend
    
    def __getitem__(self, model):
        return self._registered_models[model]
    
    def flush(self, exclude=None):
        exclude = exclude or []
        for model in self._registered_models:
            if not model._meta.name in exclude:
                model.objects.flush()
        Session.clearall()
        
    def register(self, model, backend=None, include_related=True, **params):
        if backend:
            backend = getdb(backend=backend, **params)
        registered = 0
        for model in models_from_model(model, include_related=include_related):
            if model in self._registered_models:
                continue
            registered += 1
            manager = copy.copy(model.objects)
            self._registered_models[model] = manager.register(model, backend)
            if self._install_global:
                model.objects = manager
        if registered:
            return backend
        
    def unregister(model=None):
        '''Unregister a ``model`` if provided, otherwise it unregister all
registered models.'''
        if model is not None:
            try:
                self._registered_models.pop(model)
            except KeyError:
                return 0
            return 1
        else:
            n = len(self._registered_models)
            self._registered_models.clear()
            return n
    
    def clearall(exclude=None):
        exclude = exclude or []
        results = []
        for manager in self._registered_models.values():
            if not mmanager.model._meta.name in exclude:
                results.append(model.objects.flush())
        return multi_async(results)
    
    def registered_models(self):
        return list(self._registered_models)
    
    def register_applications(self, applications, **kwargs):
        '''A simple convenience wrapper around the
:func:`stdnet.odm.register_application_models` generator.

It return s a list of registered models.'''
        return list(self.register_application_models(applications, **kwargs))
    
    def register_application_models(self, applications, models=None,
                                        app_defaults=None, default=None):
        '''A higher level registration functions for group of models located
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
            if self.register(model, include_related=False, **kwargs):
                yield model
            

#Provided for backward compatibility
global_router = Router(install_global=True)
register = global_router.register
unregister = global_router.unregister
registered_models = global_router.registered_models
register_applications = global_router.register_applications
flush_models = global_router.flush



def models_from_model(model, label=None, include_related=False, exclude=None):
    '''all model in model'''
    exclude = exclude or set()
    if model and model not in exclude:
        exclude.add(model)
        label = label or model._meta.app_label
        if not model._meta.abstract and model._meta.app_label == label:
            yield model
            if include_related:
                exclude = set(exclude or ())
                exclude.add(model)
                for field in model._meta.fields:
                    if hasattr(field, 'relmodel'):
                        for m in (field.relmodel, field.model):
                            for m in models_from_model(
                                            field.relmodel, label=label,
                                            include_related=include_related,
                                            exclude=exclude):
                                yield m

                        
def model_iterator(application, include_related=True):
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
    application = native_str(application)
    if ismodule(application) or isinstance(application, str):
        if ismodule(application):
            mod, application = application, application.__name__
        else:
            try:
                mod = import_module(application)
            except ImportError:
                # the module is not there
                mod = None
        if mod:
            label = application.split('.')[-1]
            try:
                mod_models = import_module('.models', application)
            except ImportError:
                mod_models = mod
            mod_name = mod.__name__
            label = getattr(mod_models, 'app_label', label)
            models = set()
            for name in dir(mod_models):
                value = getattr(mod_models, name)
                meta = getattr(value, '_meta', None)
                if isinstance(value, StdNetType) and meta:
                    for model in models_from_model(value, label=label,
                                            include_related=include_related):
                        if model not in models:
                            models.add(model)
                            yield model
    else:
        for app in application:
            for m in model_iterator(app):
                yield m


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

