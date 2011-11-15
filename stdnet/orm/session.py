from stdnet import getdb
from stdnet.exceptions import ModelNotRegistered

from .query import QuerySet


__all__ = ['Session','Manager']


class Session(object):
    
    def __init__(self, backend, query_class = None):
        self.backend = getdb(backend)
        self.query_class = query_class or QuerySet
    
    def __str__(self):
        return str(self.backend)
    
    def __repr__(self):
        return '{0}({1})'.format(self.__class__.__name__,self)
    
    def query(self, model):
        return QuerySet(model._meta, self.backend)
    
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
    
    
class Manager(object):
    
    def __init__(self, model = None, backend = None):
        self.register(model,backend)
        
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
    
    def register(self, model, backend = None):
        '''Register the Manager with a model and a bcakend database.'''
        self.backend = backend
        self.model = model
        
    def _get_backend(self):
        return self._session.backend if self._session else None
    def _set_backend(self, backend):
        self._session = Session(backend) if backend else None
    backend = property(_get_backend, _set_backend)
    
    @property
    def session(self):
        if not self._session:
            raise ModelNotRegistered('Model "{0}" is not registered with\
 a backend database. Use Session to query.'.format(self.model))
        return self._session
        
    def get(self, **kwargs):
        qs = self.filter(**kwargs)
        return qs.get()
    
    def flush(self):
        '''Flush the model table and all related tables including all indexes.
Calling flush will erase everything about the model
instances in the remote backend. If count is a dictionary, the method
will enumerate the number of object to delete. without deleting them.'''
        return self.session.flush(self.model)
        
    def __getattr__(self, name):
        qs = self.session.query(self.model)
        return getattr(qs,name)
    
    def __copy__(self):
        cls = self.__class__
        obj = cls.__new__(cls)
        d = self.__dict__.copy()
        d.update({'model': None, 'session': None})
        obj.__dict__ = d
        return obj