import json
from copy import copy
from itertools import chain

from stdnet import getdb
from stdnet.utils import itervalues, zip
from stdnet.utils.structures import OrderedDict
from stdnet.exceptions import ModelNotRegistered, FieldValueError, \
                                InvalidTransaction, SessionNotAvailable,\
                                CommitException

from .query import Q, Query, EmptyQuery, SessionModelBase
from .signals import *


__all__ = ['Session',
           'SessionModel',
           'Manager',
           'Transaction',
           'commit_when_no_transaction',
           'withsession']

def is_query(query):
    return isinstance(query,Q)

def commit_element_when_no_transaction(f):

    def _(self, element, modified=True):
        r = f(self, element, modified=modified)
        if modified and not self.transaction:
            self.commit()
        return r
    _.__name__ = f.__name__
    _.__doc__ = f.__doc__
    return _

def commit_when_no_transaction(f):
    '''Decorator for committing changes when the instance session is
not in a transaction.'''
    def _(self, *args, **kwargs):
        r = f(self, *args, **kwargs)
        session = self.session
        # no session available. Raise Exception
        if session is None:
            raise SessionNotAvailable('Session not available')
        session.add(self)
        return r
    _.__name__ = f.__name__
    _.__doc__ = f.__doc__
    return _

def withsession(f):
    '''Decorator for instance methods which require a :class:`Session`
available to perform their call. If a session is not available. It raises
a :class:`SessionNotAvailable` exception.'''
    def _(self, *args, **kwargs):
        if self.session is not None:
            return f(self, *args, **kwargs)
        else:
            raise SessionNotAvailable(
                        'Cannot perform operation. No session available')
    _.__name__ = f.__name__
    _.__doc__ = f.__doc__
    return _


class SessionModel(SessionModelBase):
    '''A :class:`SessionModel` is the container of all objects for a given
:class:`Model` in a stdnet :class:`Session`.'''
    def __init__(self, meta, session):
        super(SessionModel,self).__init__(meta,session)
        self._new = OrderedDict()
        self._deleted = OrderedDict()
        self._delete_query = []
        self._modified = OrderedDict()
        self._loaded = {}

    def __len__(self):
        return len(self._new) + len(self._modified) + len(self._deleted) +\
                len(self._loaded)

    def __iter__(self):
        """Iterate over all pending or persistent instances within this
Session model."""
        return iter(self.all())

    def all(self):
        return chain(self._new,self._modified,self._loaded,self._deleted)

    @property
    def model(self):
        return self.meta.model

    @property
    def new(self):
        "The set of all modified instances within this ``Session``"
        return frozenset(itervalues(self._new))

    @property
    def modified(self):
        "The set of all modified instances within this ``Session``"
        return frozenset(itervalues(self._modified))

    @property
    def loaded(self):
        '''The set of all unmodified, but not deleted, instances within this
:class:`Session`.'''
        return frozenset(itervalues(self._loaded))

    @property
    def deleted(self):
        '''The set of all instances marked as 'deleted' within this
:class:`Session`.'''
        return frozenset(itervalues(self._deleted))

    @property
    def dirty(self):
        '''The set of all instances which have changed, but not deleted,
within this :class:`Session`.'''
        return frozenset(self.iterdirty())

    def iterdirty(self):
        '''Ordered iterator over dirty elements.'''
        return iter(chain(itervalues(self._new), itervalues(self._modified)))

    def __contains__(self, instance):
        iid = instance.state().iid
        return iid in self._new or\
               iid in self._modified or\
               iid in self._deleted or\
               iid in self._loaded

    def get(self, id):
        if id in self._modified:
            return self._modified.get(id)
        elif id in self._deleted:
            return self._deleted.get(id)
        elif id in self._loaded:
            return self._loaded.get(id)

    def add(self, instance, modified = True, persistent = None):
        '''Add a new instance to the session.

:rtype: The instance added to the session'''
        state = instance.state()
        if state.deleted:
            raise ValueError('State is deleted. Cannot add.')
        self.pop(state.iid)
        persistent = persistent if persistent is not None else state.persistent
        pkname = instance._meta.pkname()
        if persistent:
            instance._dbdata[pkname] = instance.pkvalue()
        else:
            instance._dbdata.pop(pkname,None)
        state = instance.state(update = True)
        iid = state.iid
        if state.persistent:
            if modified:
                self._modified[iid] = instance
            else:
                self._loaded[iid] = instance
        else:
            self._new[iid] = instance
        return instance

    def delete(self, instance, session):
        '''delete an *instance*'''
        inst = self.pop(instance)
        instance = inst if inst is not None else instance
        if instance is not None:
            state = instance.state()
            if state.persistent:
                state.deleted = True
                self._deleted[state.iid] = instance
                instance.session = session
            else:
                instance.session = None
            return instance

    def pop(self, instance):
        '''Remove *instance* from the :class:`Session`. Instance could be a
:class:`Model` or an id.

:parameter instance: a :class:`Model` or an *id*
:rtype: the :class:`Model` removed from session or ``None`` if
    it was not in the session.
'''
        if isinstance(instance,self.meta.model):
            iid = instance.state().iid
        else:
            iid = instance
        instance = None
        for d in (self._new, self._modified, self._loaded, self._deleted):
            if iid in d:
                inst = d.pop(iid)
                if instance is None:
                    instance = inst
                elif inst is not instance:
                    raise ValueError(\
                    'Critical error. Instance {0} is duplicated'.format(iid))
        return instance

    def expunge(self, instance):
        '''Remove *instance* from the :class:`Session`. Instance could be a
:class:`Model` or an id.

:parameter instance: a :class:`Model` or an *id*
:rtype: the :class:`Model` removed from session or ``None`` if
    it was not in the session.
'''
        instance = self.pop(instance)
        instance.session = None
        return instance

    def get_delete_query(self, **kwargs):
        queries = self._delete_query
        if queries:
            q = queries[0]
            if len(queries) > 1:
                q = q.union(*queries[1:])
            return q.backend_query(**kwargs)

    def query(self):
        return self.session.query(self.model)

    def pre_commit(self, transaction):
        d = self.deleted
        if d:
            self._deleted.clear()
            if self.model._model_type == 'object':
                q = self.query().filter(id__in  = d)
                self._delete_query.append(q)
            else:
                self._delete_query.extend(d)
            if transaction.signal_delete:
                pre_delete.send(self.model, instances=self._delete_query,
                                transaction=transaction)
        dirty = tuple(self.iterdirty())
        if dirty and transaction.signal_commit:
            pre_commit.send(self.model, instances=dirty,
                            transaction=transaction)
        return len(self._delete_query) + len(dirty)

    def post_commit(self, results):
        '''\
Process results after a commit.

:parameter results: iterator over :class:`stdnet.instance_session_result`
    items.
:rtype: a two elements tuple containing a list of instances saved and
    a list of ids of instances deleted.'''
        tpy = self.meta.pk_to_python
        instances = []
        deleted = []
        errors = []
        # The length of results must be the same as the length of
        # all committed instances
        for result in results:
            if isinstance(result,Exception):
                errors.append(result.__class__(
                'Exception while commiting {0}. {1}'.format(self.meta,result)))
                continue
            instance = self.pop(result.iid)
            id = tpy(result.id)
            if result.deleted:
                deleted.append(id)
            else:
                if instance is None:
                    raise InvalidTransaction('{0} session received id "{1}"\
 which is not in the session.'.format(self,result.iid))
                setattr(instance, instance._meta.pkname(), id)
                instance = self.add(instance,
                                    modified = False,
                                    persistent = result.persistent)
                instance.state().score = result.score
                if instance.state().persistent:
                    instances.append(instance)

        return instances, deleted, errors


class SessionStructure(SessionModel):
    '''A :class:`SessionStructure` is the container of all objects for a given
:class:`Structure` in a stdnet :class:`Session`.'''
    def add(self, instance, modified = True, persistent = None):
        state = instance.state()
        state.deleted = False
        if not modified:
            self.pop(instance)
        self._modified[state.iid] = instance
        return instance


class Transaction(object):
    '''Transaction class for pipelining commands to the backend server.
An instance of this class is usually obtained via the :meth:`Session.begin`
or the :meth:`Manager.transaction` methods::

    t = session.begin()

or using the ``with`` context manager::

    with session.begin() as t:
        ...

.. attribute:: session

    :class:`Session` which is being transacted.

.. attribute:: name

    Optional :class:`Transaction` name

.. attribute:: backend

    the :class:`stdnet.BackendDataServer` to which the transaction
    is being performed.

.. attribute:: signal_commit

    If ``True``, a signal for each model in the transaction is triggered
    just after changes are committed.
    The signal carries a list of updated ``instances`` of the model,
    the :class:`Session` and the :class:`Transaction` itself.

    default ``True``.

.. attribute:: signal_delete

    If ``True`` a signal for each model in the transaction is triggered
    just after deletion of instances.
    The signal carries the list ``ids`` of deleted instances of the mdoel,
    the :class:`Session` and the :class:`Transaction` itself.

    default ``True``.

.. attribute:: logger

    Optional python logging object
'''
    default_name = 'transaction'
    commands = None
    pending = None

    def __init__(self, session, name=None,
                 signal_commit=True, signal_delete=True,
                 signal_session=True, logger=None):
        self.name = name or self.default_name
        self.session = session
        self.signal_commit = signal_commit
        self.signal_delete = signal_delete
        self.signal_session = signal_session
        self.logger = logger
        self.deleted = {}
        self.saved = {}

    @property
    def backend(self):
        if self.session is not None:
            return self.session.backend

    @property
    def is_open(self):
        return not hasattr(self,'_result')

    def add(self, instance):
        '''A convenience proxy for :meth:`Session.add` method.'''
        return self.session.add(instance)

    def delete(self, instance):
        '''A convenience proxy for :meth:`Session.delete` method.'''
        return self.session.delete(instance)
    
    def query(self, model, **kwargs):
        '''A convenience proxy for :meth:`Session.query` method.'''
        return self.session.query(model, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if type is None:
            try:
                result = self.commit()
                # allow for asynchronous results
                if result is not self:
                    self.pending = result
                return result
            except:
                self.rollback()
                raise
        else:
            self.rollback()

    def rollback(self):
        if self.session:
            self.session.expunge()
            self.session.transaction = None
            self.session = None

    def commit(self):
        '''Close the transaction and commit session to the backend.'''
        if not self.is_open:
            raise InvalidTransaction('Invalid operation.\
 Transaction already closed.')
        if self.pre_commit():
            return self.backend.execute_session(self.session, self.post_commit)
        else:
            return self.post_commit()

    def post_commit(self, response=None, commands=None):
        '''Callback from the :class:`stdnet.BackendDataServer` once the
:attr:`session` commit has finished and results are available.
Results can contain errors.

:parameter response: list of results for each :class:`SessionModel`
    in :attr:`session`. Each element in the list is a three-element tuple
    with the :attr:`SessionModel.meta` element, a list of ids and the commit
    action performed (one of ``save`` and ``delete``).
:parameter commands: The commands executed by the
    :class:`stdnet.BackendDataServer` and stored in this :class:`Transaction`
    for information.'''
        self.pending = None
        self.commands = commands
        self.result = response
        session = self.session
        self.close()
        if not response:
            return self
        signals = []
        exceptions = []
        for result in self.result:
            if isinstance(result, Exception):
                exceptions.append(result)
                continue
            meta, response = result
            if not response:
                continue
            sm = session.model(meta)
            saved, deleted, errors = sm.post_commit(response)
            exceptions.extend(errors)
            if deleted:
                self.deleted[meta] = deleted
                if self.signal_delete:
                    signals.append((post_delete.send, sm, deleted))
            if saved:
                self.saved[meta] = saved
                if self.signal_commit:
                    signals.append((post_commit.send, sm, saved))
        # Once finished we send signals
        for send, sm, instances in signals:
            send(sm.model, instances=instances, session=session,
                 transaction=self)
        if exceptions:
            failures = len(exceptions)
            if failures > 1:
                error = 'There were {0} exceptions during commit.\n\n'\
                            .format(failures)
                error += '\n\n'.join((str(e) for e in exceptions))
            else:
                error = str(exceptions[0])
            raise CommitException(error, failures=failures)
        return self

    def close(self):
        #if self.result and self.signal_session:
        #    post_commit.send(Session, transaction=self)
        for sm in self.session:
            if sm._delete_query:
                sm._delete_query = []
        self.session.transaction = None
        self.session = None

    def model(self, model):
        return self.session.model(model)

    # INTERNAL FUNCTIONS
    def pre_commit(self):
        sent = 0
        for sm in self.session:
            sent += sm.pre_commit(self)
        return sent


class Session(object):
    '''The manager of persistent operations on the backend data server for
:class:`StdModel` classes.

.. attribute:: backend

    the :class:`stdnet.BackendDataServer` instance

.. attribute:: transaction

    A :class:`Transaction` instance. Not ``None`` if this :class:`Session`
    is in a transactional state.

.. attribute:: query_class

    class for querying. Default is :class:`Query`.
'''
    _structures = {}
    def __init__(self, backend, query_class=None):
        self.backend = getdb(backend)
        self.transaction = None
        self._models = OrderedDict()
        self.query_class = query_class or Query

    def __str__(self):
        return str(self.backend)

    def __repr__(self):
        return '{0}({1})'.format(self.__class__.__name__,self)

    def __iter__(self):
        for sm in self._models.values():
            yield sm

    def __len__(self):
        return len(self._models)

    def session(self):
        '''Create a new session from this :class:`Session`'''
        return self.__class__(self.backend,self.query_class)

    @property
    def dirty(self):
        '''set of all changed instances in the session'''
        return frozenset(chain(*tuple((sm.dirty for sm\
                                        in itervalues(self._models)))))

    def model(self, meta):
        if hasattr(meta, '_meta'):
            meta = meta._meta
        sm = self._models.get(meta)
        if sm is None:
            if meta.model._model_type == 'structure':
                sm = SessionStructure(meta, self)
            else:
                sm = SessionModel(meta, self)
            self._models[meta] = sm
        return sm

    def expunge(self, instance = None):
        if instance is not None:
            sm = self._models.get(instance._meta)
            if sm:
                return sm.expunge(instance)
        else:
            self._models.clear()

    def begin(self, **options):
        '''Begin a new class:`Transaction`.
If this :class:`Session` is already within a transaction, an error is raised.'''
        if self.transaction is not None:
            raise InvalidTransaction("A transaction is already begun.")
        else:
            self.transaction = Transaction(self, **options)
        return self.transaction

    def query(self, model, query_class=None, **kwargs):
        '''Create a new :class:`Query` for *model*.'''
        query_class = query_class or self.query_class
        return query_class(model._meta, self, **kwargs)

    def empty(self, model):
        return EmptyQuery(model._meta, self)

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
        sm = self._models.get(model._meta)
        if sm:
            return sm.get(id)

    @commit_element_when_no_transaction
    def add(self, instance, modified=True):
        '''Add an *instance* to the session.

:parameter instance: a class:`StdModel` or a :class:`Structure` instance.
:parameter modified: a boolean flag indictaing if the instance was modified.

'''
        sm = self.model(instance._meta)
        instance.session = self
        return sm.add(instance, modified)

    @commit_element_when_no_transaction
    def delete(self, instance, **kwargs):
        '''Add an *instance* to the session instances to be deleted.

:parameter instance: a class:`StdModel` or a :class:`Structure` instance.
'''
        sm = self.model(instance._meta)
        # not an instance of a Model. Assume it is a query.
        if is_query(instance):
            if instance.session is not self:
                raise ValueError('Adding a query generated by another session')
            sm._delete_query.append(instance)
            return instance
        else:
            return sm.delete(instance, self)

    def flush(self, model):
        '''Completely flush a :class:`Model` from the database. No keys
associated with the model will exists after this operation.'''
        return self.backend.flush(model._meta)

    def clean(self, model):
        '''Remove empty keys for a :class:`Model` from the database. No
empty keys associated with the model will exists after this operation.'''
        return self.backend.clean(model._meta)

    def keys(self, model):
        '''Retrieve all keys for a *model*.'''
        return self.backend.model_keys(model._meta)

    def __contains__(self, instance):
        sm = self._models.get(instance._meta)
        return instance in sm if sm is not None else False

    def commit(self):
        """Commit the current transaction.
If no transaction is in progress, this method open one a."""
        if self.transaction is None:
            self.begin()
        return self.transaction.commit()

    def structure(self, instance):
        '''Return a :class:`stdnet.BackendStructure` for a given
:class:`Structure` *instance*.'''
        return self.backend.structure(instance)

    @classmethod
    def clearall(cls):
        pass


class Manager(object):
    '''A manager class for models. Each :class:`StdModel`
contains at least one manager which can be accessed via the ``objects``
class attribute::

    class MyModel(odm.StdModel):
        group = odm.SymbolField()
        flag = odm.BooleanField()

    MyModel.objects

Managers are used as :class:`Session` and :class:`Query` factories
for a given :class:`StdModel`::

    session = MyModel.objects.session()
    query = MyModel.objects.query()

.. attribute:: model

    The :class:`StdModel` for this :class:`Manager`. This attribute is
    assigned by the Object relational mapper at runtime.

.. attribute:: backend

    The :class:`stdnet.BackendDataServer` for this :class:`Manager`.

'''
    def __init__(self, model=None, backend=None):
        self.register(model, backend)

    def register(self, model, backend=None):
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

    def session(self, transaction=None):
        '''Returns a new :class:`Session`.'''
        if transaction:
            return transaction.session
        elif not self.backend:
            raise ModelNotRegistered("Model '{0}' is not registered with a\
 backend database. Cannot use manager.".format(self.model._meta))
        return Session(self.backend)

    def transaction(self, *models, **kwargs):
        '''Return a :class:`Transaction`. If models are specified, it check
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
        return self.session().begin(**kwargs)

    # SESSION Proxy methods
    def query(self, transaction=None):
        '''Returns a new :class:`Query` for the :attr:`Manager.model`.'''
        return self.session(transaction=transaction).query(self.model)

    def empty(self):
        '''Returns an empty :class;`Query`'''
        return self.session().empty(self.model)

    def filter(self, **kwargs):
        return self.query().filter(**kwargs)

    def exclude(self, **kwargs):
        return self.query().exclude(**kwargs)

    def search(self, text, lookup = None):
        return self.query().search(text, lookup = lookup)

    def get(self, **kwargs):
        return self.query().get(**kwargs)

    def flush(self):
        return self.session().flush(self.model)

    def clean(self):
        return self.session().clean(self.model)

    def keys(self):
        return self.session().keys(self.model)

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
        try:
            value = getattr(model,name)
        except ModelNotRegistered:
            continue
        if name != 'objects' and isinstance(value,Manager):
            managers.append(new_manager(model,name,value))
    model._managers = managers