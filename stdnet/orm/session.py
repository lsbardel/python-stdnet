import json
from copy import copy

from stdnet import getdb
from stdnet.utils import itervalues
from stdnet.utils.structures import OrderedDict
from stdnet.exceptions import ModelNotRegistered, FieldValueError, \
                                InvalidTransaction

from .query import Query
from .signals import *


__all__ = ['Session','Manager']


class Session(object):
    '''The manager of persistent operations on the backend data server for
:class:`StdModel` classes.

.. attribute:: backend

    the :class:`stdnet.BackendDataServer` instance
'''
    def __init__(self, backend, query_class = None):
        self.backend = getdb(backend)
        self.transaction = None
        self._new = OrderedDict()
        self._deleted = OrderedDict()
        self._modified = OrderedDict()
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
        return frozenset(self._deleted.values())
    
    def __str__(self):
        return str(self.backend)
    
    def __repr__(self):
        return '{0}({1})'.format(self.__class__.__name__,self)
    
    def begin(self, subtransactions=False, nested=False):
        '''Begin a class:`stdnet.Transaction` for models.
If this Session is already within a transaction, either a plain
transaction or nested transaction, an error is raised, unless
``subtransactions=True`` or ``nested=True`` is specified.

The ``subtransactions=True`` flag indicates that this :meth:`transaction` 
can create a subtransaction if a transaction is already in progress.
For documentation on subtransactions, please see :ref:`session_subtransactions`.

The ``nested`` flag begins a SAVEPOINT transaction and is equivalent
to calling :meth:`~.Session.begin_nested`. For documentation on SAVEPOINT
transactions, please see :ref:`session_begin_nested`.'''
        if self.transaction is not None:
            if subtransactions or nested:
                self.transaction = self.transaction._begin(nested=nested)
            else:
                raise InvalidTransaction(
                    "A transaction is already begun.  Use subtransactions=True "
                    "to allow subtransactions.")
        else:
            self.transaction = self.backend.transaction(session = self)
        return self.transaction
    
    def query(self, model, **kwargs):
        return self.query_class(model._meta, self, **kwargs)
    
    def get(self, model, **kwargs):
        qs = self.query(model, fargs=kwargs)
        return qs.get()
    
    def get_or_create(self, model, **kwargs):
        '''Get an object. If it does not exists, it creates one'''
        try:
            res = self.get(model, **kwargs)
            created = False
        except model.DoesNotExist:
            res = self.save(model(**kwargs))
            created = True
        return res,created
    
    def add(self, instance):
        '''Add an *instance* to the session.'''
        state = instance.state()
        if state.deleted:
            raise ValueError('State is deleted. Cannot add.')
        if state.persistent:
            self._modified[state] = instance
        else:
            self._new[state] = instance
        
        return state
    
    def delete(self, instance):
        if instance.uuid in self._deleted:
            return
        self._deleted[instance.uuid] = instance
        
    def flush(self, model):
        return self.backend.flush(model._meta)
    
    def __contains__(self, instance):
        state = instance.state()
        return state in self._new or state in self._deleted\
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
        if self.transaction:
            return self.transaction.commit()
        else:
            raise InvalidTransaction('No transaction was started')
        
        
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
            if self._session:
                return '{0}({1} - {2})'.format(self.__class__.__name__,
                                               self.model,
                                               self.session)
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
        return self.query()
    
    def filter(self, **kwargs):
        return self.query().filter(**kwargs)
    
    def exclude(self, **kwargs):
        return self.query().exclude(**kwargs)
    
    def search(self, text):
        return self.query().search(text)
    
    def flush(self):
        return self.session().flush(self.model)
    
    def get(self, **kwargs):
        return self.session().get(self.model, fargs = kwargs)
    
    def get_or_create(self, **kwargs):
        return self.session().get_or_create(self.model, **kwargs)
    
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