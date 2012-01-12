import json
from copy import copy

from stdnet import getdb
from stdnet.utils import itervalues
from stdnet.utils.structures import OrderedDict
from stdnet.exceptions import ModelNotRegistered, FieldValueError, \
                                InvalidTransaction

from .query import Query
from .signals import *


__all__ = ['Session','Manager','Transaction']


class Transaction(object):
    '''Transaction class for pipelining commands to the back-end.
An instance of this class is usually obtained by using the high level
:func:`stdnet.transaction` function.

.. attribute:: name

    Transaction name
    
.. attribute:: backend

    the :class:`BackendDataServer` to which the transaction is being performed.
    
.. attribute:: cursor

    The backend cursor which manages the transaction.
    
    '''
    default_name = 'transaction'
    
    def __init__(self, session, name = None):
        self.name = name or self.default_name
        self.session = session
        self._callbacks = []
        
    @property
    def backend(self):
        return self.session.backend
    
    @property
    def is_open(self):
        return not hasattr(self,'_result')
    
    def add(self, func, args, kwargs, callback = None):
        '''Add an new operation to the transaction.

:parameter func: function to call which accept :attr:`stdnet.Transaction.cursor`
    as its first argument.
:parameter args: tuple or varying arguments to pass to *func*.
:parameter kwargs: dictionary or key-values arguments to pass to *func*.
:parameter callback: optional callback function with arity 1 which process
    the result wonce back from the backend.'''
        res = func(self.cursor,*args,**kwargs)
        callback = callback or default_callback
        self._callbacks.append(callback)
        
    def merge(self, other):
        '''Merge two transaction together'''
        if self.backend == other.backend:
            if not other.empty():
                raise NotImplementedError()
        else:
            raise InvalidTransaction('Cannot merge transactions.')
        
    def empty(self):
        '''Check if the transaction contains query data or not.
If there is no query data the transaction does not perform any
operation in the database.'''
        for c in itervalues(self._cachepipes):
            if c.pipe:
                return False
        return self.emptypipe()
                
    def emptypipe(self):
        raise NotImplementederror
            
    def structure_pipe(self, structure):
        '''Create a pipeline for a structured datafield'''
        id = structure.id
        if id not in self._cachepipes:
            self._cachepipes[id] = structure.struct()
        return self._cachepipes[id].pipe
    
    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        if type is None:
            self.commit()
        else:
            self.rollback()
            
    def rollback(self):
        pass
            
    def commit(self):
        '''Close the transaction and commit commands to the backend.'''
        if not self.is_open:
            raise InvalidTransaction('Invalid operation.\
 Transaction already closed.')
        results = self.backend.execute_session(self.session)
        callbacks = self._callbacks
        self._callbacks = []
        if len(results) and len(callbacks):
            self._results = ((cb(r) for cb,r in\
                                   zip(callbacks,results)))
        else:
            self._results = results
        self.close()
    
    def close(self):
        self.session.transaction = None
        self.session = None

    # VIRTUAL FUNCTIONS
    
    def _execute(self):
        raise NotImplementedError


class Session(object):
    '''The manager of persistent operations on the backend data server for
:class:`StdModel` classes.

.. attribute:: backend

    the :class:`stdnet.BackendDataServer` instance
    
.. attribute:: autocommit

    When ``True``, the :class:`Session`` does not keep a persistent transaction
    running, and will acquire connections from the engine on an as-needed basis,
    returning them immediately after their use.
    Flushes will begin and commit (or possibly rollback) their own transaction
    if no transaction is present. When using this mode, the :meth:`begin`
    method may be used to begin a transaction explicitly.
          
    Default: ``False``. 
          
.. attribute:: transaction

    A :class:`Transaction` instance. Not ``None`` if this :class:`Session`
    is in a transactional state.
    
.. attribute:: query_class

    class for querying. Default is :class:`Query`.
'''
    _structures = {}
    def __init__(self, backend, autocommit = False, query_class = None):
        self.backend = getdb(backend)
        self.transaction = None
        self.autocommit = autocommit
        self._new = OrderedDict()
        self._deleted = OrderedDict()
        self._modified = OrderedDict()
        self._loaded = {}
        self.query_class = query_class or Query
    
    @property
    def new(self):
        "The set of all new instances within this ``Session``"
        return frozenset(self._new.values())
    
    @property
    def modified(self):
        "The set of all modified instances within this ``Session``"
        return frozenset(self._modified.values())
    
    @property
    def deleted(self):
        "The set of all instances marked as 'deleted' within this ``Session``"
        return frozenset(self._loaded.values())
    
    @property
    def deleted(self):
        "The set of all instances marked as 'deleted' within this ``Session``"
        return frozenset(self._deleted.values())
    
    def __str__(self):
        return str(self.backend)
    
    def __repr__(self):
        return '{0}({1})'.format(self.__class__.__name__,self)
    
    def begin(self):
        '''Begin a new class:`Transaction`.
If this :class:`Session` is already within a transaction, an error is raised.'''
        if self.transaction is not None:
            raise InvalidTransaction("A transaction is already begun.")
        else:
            self.transaction = Transaction(self)
        return self.transaction
    
    def query(self, model):
        '''Create a new :class:`Query` for *model*.'''
        return self.query_class(model._meta, self)
    
    def get_or_create(self, model, **kwargs):
        '''Get an instance of *model* from the internal cache (only if the
dictionary *kwargs* is of length 1 and has key given by ``id``) or from the
server. If it the instance is not available, it tries to create one
from the **kwargs** parameters.

:parameter model: a :class:`StdModel`
:parameter kwargs: dictionary of parameters.
:rtype: an instance of  two elements tuple containing the instance and a boolean
    indicating if the instance was created or not.
'''
        try:
            res = self.query(model).get(**kwargs)
            created = False
        except model.DoesNotExist:
            res = self.add(model(**kwargs))
            created = True
        return res,created

    def get(self, model, id):
        uuid = model.get_uuid(id)
        if uuid in self._modified:
            return self._modified.get(uuid)
        elif uuid in self._deleted:
            return self._modified.get(uuid)
        
    def add(self, instance, modified = True):
        '''Add an *instance* to the session.
        
:parameter instance: a class:`StdModel` or a :class:`Structure` instance.
:parameter modified: a boolean flag indictaing if the instance was modified.
    
'''
        state = instance.state()
        if state.deleted:
            raise ValueError('State is deleted. Cannot add.')
        instance.session = self
        if state.persistent:
            if modified:
                self._loaded.pop(state,None)
                self._modified[instance] = instance
            elif state not in self._modified:
                self._loaded[instance] = instance
        else:
            self._new[instance] = instance
        
        return instance
    
    def delete(self, instance):
        self.expunge(instance)
        if instance.state().persistent:
            self._deleted[instance] = instance
        
    def flush(self, model):
        return self.backend.flush(model._meta)
    
    def __contains__(self, instance):
        return instance in self._new or state in self._deleted\
                                     or state in self._modified
                                  
    def __iter__(self):
        """Iterate over all pending or persistent instances within this
Session."""
        for v in self._new:
            yield v
        for m in self._modified:
            yield m
        for d in self._deleted:
            yield d
        
    def commit(self):
        """Flush pending changes and commit the current transaction.

        If no transaction is in progress, this method raises an
        InvalidRequestError.

        By default, the :class:`.Session` also expires all database
        loaded state on all ORM-managed attributes after transaction commit.
        This so that subsequent operations load the most recent 
        data from the database.   This behavior can be disabled using
        the ``expire_on_commit=False`` option to :func:`.sessionmaker` or
        the :class:`.Session` constructor.

        If a subtransaction is in effect (which occurs when begin() is called
        multiple times), the subtransaction will be closed, and the next call
        to ``commit()`` will operate on the enclosing transaction.

        For a session configured with autocommit=False, a new transaction will
        be begun immediately after the commit, but note that the newly begun
        transaction does *not* use any connection resources until the first
        SQL is actually emitted.

        """
        if self.transaction is None:
            if not self.autocommit:
                self.begin()
            else:
                raise InvalidTransaction('No transaction was started')

        self.transaction.commit()
        
    def expunge(self, instance):
        instance._dbdata.pop('state',None)
        for d in (self._new,self._modified,self._loaded,self._deleted):
            if instance in d:
                d.pop(instance)
                return True
        return False
    
    def server_update(self, instance, id = None):
        '''Callback by the :class:`stdnet.BackendDataServer` once the commit is
finished. Remove the deleted instances and updated the modified and new
instances.'''
        state = instance.state()
        self.expunge(instance)
        if not state.deleted:
            if id:
                instance.id = instance._meta.pk.to_python(id)
                instance._dbdata['id'] = instance.id
            self.add(instance, False)
            return instance
        
    @classmethod
    def clearall(cls):
        pass
      
        
class Manager(object):
    '''A manager class for models. Each :class:`StdModel`
class contains at least one manager which can be accessed by the ``objects``
class attribute::

    class MyModel(orm.StdModel):
        group = orm.SymbolField()
        flag = orm.BooleanField()
        
    MyModel.objects

Managers are shortcut of :class:`Session` instances for a model class.
Managers are used to construct queries for object retrieval.
Queries can be constructed by selecting instances with specific fields
using a where or limit clause, or a combination of them::

    MyModel.objects.filter(group = 'bla')
    
    MyModel.objects.filter(group__in = ['bla','foo'])

    MyModel.objects.filter(group__in = ['bla','foo'], flag = True)
    
They can also exclude instances from the query::

    MyModel.objects.exclude(group = 'bla')
'''
    def __init__(self, model = None, backend = None):
        self.register(model, backend)
    
    def register(self, model, backend = None):
        '''Register the Manager with a model and a backend database.'''
        self.backend = backend
        self.model = model
    
    def __str__(self):
        if self.model:
            if self.backend:
                return '{0}({1} - {2})'.format(self.__class__.__name__,
                                               self.model,
                                               self.backend)
            else:
                return '{0}({1})'.format(self.__class__.__name__,self.model)
        else:
            return self.__class__.__name__
    __repr__ = __str__

    def session(self):
        if not self.backend:
            raise ModelNotRegistered("Model '{0}' is not registered with a\
 backend database. Cannot use manager.".format(self.model))
        return Session(self.backend)
    
    def transaction(self, *models):
        '''Return a transaction instance. If models are specified, it check
if their managers have the same backend database.'''
        backend = self.backend
        for model in models:
            c = model.objects.backend
            if not c:
                raise ModelNotRegistered("Model '{0}' is not registered with a\
     backend database. Cannot start a transaction.".format(model))
            if backend and backend != c:
                raise InvalidTransaction("Models {0} are registered\
     with a different databases. Cannot create transaction"\
                .format(', '.join(('{0}'.format(m) for m in models))))
        return self.session().begin()
    
    # SESSION Proxy methods
    def query(self):
        return self.session().query(self.model)
    
    def all(self):
        return self.query().all()
    
    def filter(self, **kwargs):
        return self.query().filter(**kwargs)
    
    def exclude(self, **kwargs):
        return self.query().exclude(**kwargs)
    
    def search(self, text):
        return self.query().search(text)
    
    def get(self, **kwargs):
        return self.query().get(**kwargs)
    
    def flush(self):
        return self.session().flush(self.model)
    
    def get_or_create(self, **kwargs):
        session = self.session()
        with session.begin():
            el,created = session.get_or_create(self.model, **kwargs)
        return el,created
    
    def __copy__(self):
        cls = self.__class__
        obj = cls.__new__(cls)
        d = self.__dict__.copy()
        d.update({'model': None, '_session': None})
        obj.__dict__ = d
        return obj
        

def new_manager(model, name, manager):
    if manager is None:
        manager = Manager()
    else:
        manager = copy(manager)
    manager.register(model)
    setattr(model, name, manager)
    return manager
            

def setup_managers(model):
    managers = []
    # the default manager is handled first
    objects = getattr(model,'objects',None)
    managers.append(new_manager(model,'objects',objects))
    for name in dir(model):
        value = getattr(model,name)
        if name != 'objects' and isinstance(value,Manager):
            managers.append(new_manager(model,name,value))
    model._managers = managers