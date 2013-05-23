from inspect import ismodule, isclass

from stdnet.utils import native_str
from stdnet.utils.async import multi_async
from stdnet.utils.importer import import_module
from stdnet.utils.dispatch import Signal
from stdnet import getdb

from .base import ModelType, Model
from .session import Manager, Session, ModelDictionary, StructureManager
from .struct import Structure
from .globals import get_model_from_hash

__all__ = ['Router', 'model_iterator']
        
        
class Router(object):
    '''A router is a mapping of :class:`Model` to the registered
:class:`Manager` of that model::
    
    from stdnet import odm
    
    models = odm.Router()
    models.register(MyModel, ...)
    
    # dictionary Notation
    query = models[MyModel].query()
    
    # or dotted notation (lowercase)
    query = models.mymodel.query()
    
The ``models`` instance in the above snipped can be set globally if
one wishes to do so.

.. attribute:: pre_commit

    A signal which can be used to register ``callbacks`` before instances are
    committed::
    
        models.pre_commit.connect(callback, sender=MyModel)
    
.. attribute:: pre_delete

    A signal which can be used to register ``callbacks`` before instances are
    deleted::
    
        models.pre_delete.connect(callback, sender=MyModel)
        
.. attribute:: post_commit

    A signal which can be used to register ``callbacks`` after instances are
    committed::
    
        models.post_commit.connect(callback, sender=MyModel)
        
.. attribute:: post_delete

    A signal which can be used to register ``callbacks`` after instances are
    deleted::
    
        models.post_delete.connect(callback, sender=MyModel)
'''
    def __init__(self, default_backend=None, install_global=False):
        self._registered_models = ModelDictionary()
        self._registered_names = {}
        self._default_backend = default_backend
        self._install_global = install_global
        self._structures = {}
        self._search_engine = None
        self.pre_commit = Signal(providing_args=["instances", "session"])
        self.pre_delete = Signal(providing_args=["instances", "session"])
        self.post_commit = Signal(providing_args=["instances", "session"])
        self.post_delete = Signal(providing_args=["instances", "session"])
        
    @property
    def default_backend(self):
        '''The default backend for this :class:`Router`. This is used when
calling the :meth:`register` method without explicitly passing a backend.'''
        return self._default_backend
        
    @property
    def registered_models(self):
        '''List of registered :class:`Model`.'''
        return list(self._registered_models)
    
    @property
    def search_engine(self):
        '''The :class:`SearchEngine` for this :class:`Router`. This
must be created by users. Check :ref:`full text search <tutorial-search>`
tutorial for information.'''
        return self._search_engine
    
    def __repr__(self):
        return '%s %s' % (self.__class__.__name.__, self._registered_models)
    
    def __str__(self):
        return str(self._registered_models)
    
    def __contains__(self, model):
        return model in self._registered_models
    
    def __getitem__(self, model):
        return self._registered_models[model]
    
    def __getattr__(self, name):
        if name in self._registered_names:
            return self._registered_names[name]
        raise AttributeError('No model named "%s"' % name)
    
    def structure(self, model):
        return self._structures.get(model)            
    
    def set_search_engine(self, engine):
        '''Set the search ``engine`` for this :class:`Router`.'''
        self._search_engine = engine
        self._search_engine.set_router(self)
        
    def register(self, model, backend=None, read_backend=None,
                 include_related=True, **params):
        '''Register a :class:`Model` with this :class:`Router`. If the
model was already registered it does nothing.

:param model: a :class:`Model` class.
:param backend: a :class:`stdnet.BackendDataServer` or a
    :ref:`connection string <connection-string>`.
:param read_backend: Optional :class:`stdnet.BackendDataServer` for read
    operations. This is useful when the server has a master/slave
    configuration, where the master accept write and read operations
    and the ``slave`` read only operations.
:param include_related: ``True`` if related models to ``model`` needs to be
    registered. Default ``True``.
:param params: Additional parameters for the :func:`getdb` function.
:return: the number of models registered.
'''
        backend = backend or self._default_backend
        backend = getdb(backend=backend, **params)
        if read_backend:
            read_backend = getdb(read_backend)
        registered = 0
        if isinstance(model, Structure):
            self._structures[model] = StructureManager(model, backend,
                                                       read_backend, self)
            return model
        for model in models_from_model(model, include_related=include_related):
            if model in self._registered_models:
                continue
            registered += 1
            default_manager = backend.default_manager or Manager
            manager_class = getattr(model, 'manager_class', default_manager)
            manager = manager_class(model, backend, read_backend, self)
            self._registered_models[model] = manager
            if isinstance(model, ModelType):
                attr_name = model._meta.name
            else:
                attr_name = model.__name__.lower()
            if attr_name not in self._registered_names:
                self._registered_names[attr_name] = manager
            if self._install_global:
                model.objects = manager
        if registered:
            return backend
    
    def from_uuid(self, uuid, session=None):
        '''Retrieve a :class:`Model` from its universally unique identifier
``uuid``. If the ``uuid`` does not match any instance an exception will raise.'''
        elems = uuid.split('.')
        if len(elems) == 2:
            model = get_model_from_hash(elems[0])
            if not model:
                raise Model.DoesNotExist(\
                            'model id "{0}" not available'.format(elems[0]))
            if not session or session.router is not self:
                session = self.session()
            return session.query(model).get(id=elems[1])
        raise Model.DoesNotExist('uuid "{0}" not recognized'.format(uuid))

    def flush(self, exclude=None):
        '''Flush all :attr:`registered_models` excluding the ones
in ``exclude`` (if provided).'''
        exclude = exclude or []
        results = []
        for manager in self._registered_models.values():
            m = manager._meta
            if not (m.name in exclude or m.modelkey in exclude):
                results.append(manager.flush())
        return multi_async(results)
        
    def unregister(self, model=None):
        '''Unregister a ``model`` if provided, otherwise it unregister all
registered models. Return a list of unregistered model managers or ``None``
if no managers were removed.'''
        if model is not None:
            try:
                manager = self._registered_models.pop(model)
            except KeyError:
                return
            if self._registered_names.get(manager._meta.name) == manager:
                self._registered_names.pop(manager._meta.name)
            return [manager]
        else:
            managers = list(self._registered_models.values())
            self._registered_models.clear()
            return managers
    
    def register_applications(self, applications, models=None, backends=None):
        '''A higher level registration functions for group of models located
on application modules.
It uses the :func:`model_iterator` function to iterate
through all :class:`Model` models available in ``applications``
and register them using the :func:`register` low level method.

:parameter applications: A String or a list of strings representing
    python dotted paths where models are implemented.
:parameter models: Optional list of models to include. If not provided
    all models found in *applications* will be included.
:parameter backends: optional dictionary which map a model or an
    application to a backend :ref:`connection string <connection-string>`.
:rtype: A list of registered :class:`Model`.

For example::

    
    mapper.register_application_models('mylib.myapp')
    mapper.register_application_models(['mylib.myapp', 'another.path'])
    mapper.register_application_models(pythonmodule)
    mapper.register_application_models(['mylib.myapp',pythonmodule])

'''
        return list(self._register_applications(applications, models, backends))
    
    def session(self):
        '''Obatain a new :class:`Session` for this ``Router``.'''
        return Session(self)
        
    def create_all(self):
        '''Loop though :attr:`registered_models` and issue the
:meth:`Manager.create_all` method.'''
        for manager in self._registered_models.values():
            manager.create_all()
        
    def add(self, instance):
        '''Add an ``instance`` to its backend database. This is a shurtcut
method for::

    self.session().add(instance)
'''
        return self.session().add(instance)
    
    # PRIVATE METHODS
    
    def _register_applications(self, applications, models, backends):
        backends = backends or {}
        for model in model_iterator(applications):
            name = str(model._meta)
            if models and name not in models:
                continue
            if name not in backends:
                name = model._meta.app_label
            kwargs = backends.get(name, self._default_backend)
            if not isinstance(kwargs, dict):
                kwargs = {'backend': kwargs}
            else:
                kwargs = kwargs.copy()
            if self.register(model, include_related=False, **kwargs):
                yield model


def models_from_model(model, include_related=False, exclude=None):
    '''Generator of all model in model.'''
    exclude = exclude or set()
    if model and model not in exclude:
        exclude.add(model)
        if isinstance(model, ModelType) and not model._meta.abstract:
            yield model
            if include_related:
                exclude = set(exclude or ())
                exclude.add(model)
                for field in model._meta.fields:
                    if hasattr(field, 'relmodel'):
                        for m in (field.relmodel, field.model):
                            for m in models_from_model(
                                            field.relmodel,
                                            include_related=include_related,
                                            exclude=exclude):
                                yield m
                for manytomany in model._meta.manytomany:
                    related = getattr(model, manytomany)
                    for m in models_from_model(related.model,
                                               include_related=include_related,
                                               exclude=exclude):
                        yield m
        elif not isinstance(model, ModelType) and isclass(model):
            # This is a class which is not o ModelType
            yield model

                        
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
            label = getattr(mod_models, 'app_label', label)
            models = set()
            for name in dir(mod_models):
                value = getattr(mod_models, name)
                meta = getattr(value, '_meta', None)
                if isinstance(value, ModelType) and meta:
                    for model in models_from_model(value, 
                                            include_related=include_related):
                        if model._meta.app_label == label\
                            and model not in models:
                            models.add(model)
                            yield model
    else:
        for app in application:
            for m in model_iterator(app):
                yield m
