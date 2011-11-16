import json

from stdnet import getdb
from stdnet.exceptions import ModelNotRegistered, FieldValueError

from .query import QuerySet
from .signals import *


__all__ = ['Session','Manager']


class Session(object):
    
    def __init__(self, backend, query_class = None):
        self.backend = getdb(backend)
        self.query_class = query_class or QuerySet
    
    def __str__(self):
        return str(self.backend)
    
    def __repr__(self):
        return '{0}({1})'.format(self.__class__.__name__,self)
    
    def query(self, model, fargs=None, eargs=None):
        return QuerySet(model._meta, self.backend, fargs=fargs, eargs=eargs)
    
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
    
    def save(self, instance, transaction = None):
        '''Save instance in the backend data server.'''
        backend = self.backend
        if not instance.is_valid(backend):
            raise FieldValueError(json.dumps(instance.errors))
        commit = False
        if not transaction:
            commit = True
            transaction = instance.local_transaction()
        dbdata = instance._dbdata
        idnew = True
        
        # This is an instanceect we got from the database
        if  instance.id and 'id' in dbdata:
            idnew = instance.id != dbdata['id']
            if idnew:
                raise ValueError('Id has changed from {0} to {1}.'\
                                 .format(instance.id,dbdata['id']))
        elif not instance.id:
            instance.id = instance._meta.pk.serialize(instance.id, backend)
            
        instance = backend.save_object(instance, idnew, transaction)
        if commit:
            transaction.commit()
            instance._dbdata.update(instance.cleaned_data)
            instance._dbdata['id'] = instance.id
        
        return instance
    
    def delete(self, instance, transaction = None):
        if not instance.id:
            raise FieldValueError('Cannot delete object. It was never saved.')
        T = 0
        # Gather related objects to delete
        pre_delete.send(sender=instance.__class__, instance = instance)
        for obj in instance.related_objects():
            T += self.delete(obj, transaction)
        res = T + self.backend.delete_object(instance, transaction)
        post_delete.send(sender=instance.__class__, instance = instance)
        return res
    
    def flush(self, model):
        return self.backend.flush(model._meta)
    
    def transaction(*models, **kwargs):
        '''Create a transaction'''
        if not models:
            raise ValueError('Cannot create transaction with no models')
        cursor = None
        tra = None
        for model in models:
            c = model._meta.cursor
            if not c:
                raise ModelNotRegistered("Model '{0}' is not registered with a\
     backend database. Cannot start a transaction.".format(model))
            if cursor and cursor != c:
                raise InvalidTransaction("Models {0} are registered\
     with a different databases. Cannot create transaction"\
                .format(', '.join(('{0}'.format(m) for m in models))))
            cursor = c
            # Check for local transactions
            if hasattr(model,attr_local_transaction):
                t = getattr(model,attr_local_transaction)
                if tra:
                    tra.merge(t)
                else:
                    tra = t
        return tra or cursor.transaction(**kwargs)
    
    def all(self, model):
        return self.query(model)
    
    def filter(self, model, **kwargs):
        return self.query(model, fargs = kwargs)
    
    def exclude(self, model, **kwargs):
        return self.query(model, eargs = kwargs)
        
    
class SessionProxy(object):
    __slots__ = ('model','session','attr')
    
    def __init__(self, model, session, attr):
        self.model = model
        self.session = session
        self.attr = attr
        
    def __call__(self, *args, **kwargs):
        return getattr(self.session,self.attr)(self.model, *args, **kwargs)
        
        
class ManagerMixin(object):
    
    def delete(self, *args, **kwargs):
        return self.session.delete(*args, **kwargs)
    
    def save(self, *args, **kwargs):
        return self.session.delete(*args, **kwargs)
    
    def _get_backend(self):
        return self._session.backend if self._session else None
    def _set_backend(self, backend):
        self._session = Session(backend) if backend else None
    backend = property(_get_backend, _set_backend)
    
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

    @property
    def session(self):
        if not self._session:
            raise ModelNotRegistered('Model "{0}" is not registered with\
 a backend database. Use Session to query.'.format(self.model))
        return self._session
    
    def __getattr__(self, name):
        return SessionProxy(self.model,self.session,name)
    
    def __copy__(self):
        cls = self.__class__
        obj = cls.__new__(cls)
        d = self.__dict__.copy()
        d.update({'model': None, '_session': None})
        obj.__dict__ = d
        return obj
    
    
    
class SessionModel(ManagerMixin):
    __slots__ = ('model','_session')
    
    def __init__(self, model, backend):
        self.model = model
        self._session = Session(backend)
    
    
class Manager(ManagerMixin):
    '''A manager class for models. Each :class:`stdnet.orm.StdModel`
class contains at least one manager which can be accessed by the ``objects``
class attribute::

    class MyModel(orm.StdModel):
        group = orm.SymbolField()
        flag = orm.BooleanField()
        
    MyModel.objects

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
        self.register(model,backend)
    
    def register(self, model, backend = None):
        '''Register the Manager with a model and a bcakend database.'''
        self.backend = backend
        self.model = model
        