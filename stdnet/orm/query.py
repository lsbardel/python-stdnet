from copy import copy
from collections import namedtuple

from stdnet.exceptions import *
from stdnet.utils import zip, to_bytestring

from .transactions import transaction as get_transaction


queryarg = namedtuple('queryarg','name values unique lookup')
field_query = namedtuple('field_query','query field')


class EmptySet(frozenset):
    query_set = ''
    
    def items(self, slic):
        raise StopIteration
    
    def count(self):
        return len(self)
    

class QuerySet(object):
    '''A QuerySet is not created on its but instead using the model manager.
For example::

    qs = MyModel.objects.filter(field = 'bla)
    
``qs`` is a queryset instance for model ``MyModel``.
'''
    start = None
    stop = None
    lookups = ('in','contains')
    
    def __init__(self, meta, fargs = None, eargs = None,
                 filter_sets = None, ordering = None,
                 queries = None, empty = False):
        '''\
Initialize a queryset. The constructor is not called directly since
instances of queryset are constructued using the 
:class:`stdnet.orm.query.Manager` instance of a model.

.. attribute:: _meta

    an model instance meta attribute,
    
.. attribute:: fargs

    dictionary containing the lookup parameters to include.
    
.. attribute:: eargs

    dictionary containing the lookup parameters to exclude from query.

.. attribute:: ordering

    optional ordering field.
'''
        self._meta  = meta
        self.model  = meta.model
        self.fargs  = fargs
        self.eargs  = eargs
        self.ordering = ordering
        self.filter_sets = filter_sets
        self.queries = queries
        self._select_related = None
        self.clear()
        if empty:
            self.qset = EmptySet()
        
    def clear(self):
        self.simple = False
        self.qset   = None
        self._seq   = None
        
    def __repr__(self):
        if self._seq is None:
            s = self.__class__.__name__
            if self.fargs:
                s = '%s.filter(%s)' % (s,self.fargs)
            if self.eargs:
                s = '%s.exclude(%s)' % (s,self.eargs)
            return s
        else:
            return str(self._seq)
    __str__ = __repr__
    
    def __getitem__(self, slic):
        if isinstance(slic,slice):
            return self.aslist(slic)
        return self.aslist()[slic]
    
    def all(self):
        return self
    
    def __iter__(self):
        return self.items()
    
    def _clone(self, fargs, eargs):
        return self.__class__(self._meta,
                              fargs=fargs,
                              eargs=eargs,
                              filter_sets=self.filter_sets,
                              ordering=self.ordering,
                              queries=self.queries)
        
    def filter(self, **kwargs):
        '''Returns a new ``QuerySet`` containing objects that match the\
 given lookup parameters.'''
        if kwargs:
            if self.fargs:
                c = self.fargs.copy()
                c.update(kwargs)
                kwargs = c
            return self._clone(kwargs, self.eargs)
        else:
            return self
    
    def exclude(self, **kwargs):
        '''Returns a new ``QuerySet`` containing objects that do not match\
 the given lookup parameters.'''
        if kwargs:
            if self.eargs:
                c = self.eargs.copy()
                c.update(kwargs)
                kwargs = c
            return self._clone(self.fargs,kwargs)
        else:
            return self
    
    def sort_by(self, ordering):
        '''Sort the query by the given field'''
        if ordering:
            ordering = self._meta.get_sorting(ordering,QuerySetError)
        self.ordering = ordering
        return self
    
    def search(self, text):
        '''Search text in model. A search engine needs to be installed
for this function to be available.'''
        se = self._meta.searchengine
        if se:
            return se.search_model(self.model,text)
        else:
            raise QuerySetError('Search not implemented for {0} model'\
                                .format(self.model))
            
    def load_related(self, *fields):
        '''Return a new ``QuerySet`` that automatically follow foreign-key
relationships, selecting that additional related-object data when it executes
its query.

:parameter fields: fields to include in the loading. If not provided all
    foreign key will be loaded.
:rtype: an instance of a :class:`stdnet.orm.query.Queryset`.'''
        if not fields:
            fields = []
            for field in self._meta.scalarfields:
                if hasattr(field,'relmodel'):
                    fields.append(field)
            fields.extend(self._meta.multifields)
        else:
            fields = [self._meta.dfields[field] for field in fields]
        self._select_related = fields
        return self
    
    def get(self):
        items = self.aslist()
        if items:
            if len(items) == 1:
                return items[0]
            else:
                raise QuerySetError('Get query {0} yielded non\
 unique results'.format(self))
        else:
            raise self.model.DoesNotExist
    
    def count(self):
        '''Return the number of objects in ``self``.
This method is efficient since the queryset does not
receive any data from the server. It construct the queries and count the
objects on the server side.'''
        self._buildquery()
        return self.qset.count()
    
    def querykey(self):
        self.count()
        return self.qset.query_set
        
    def __contains__(self, val):
        if isinstance(val,self.model):
            val = val.id
        try:
            val = to_bytestring(val)
        except:
            return False
        self._buildquery()
        return self.qset.has(val)
        
    def __len__(self):
        return self.count()
    
    # PRIVATE METHODS
    
    def _buildquery(self):
        # Build a queryset from filters and exclude arguments
        if self.qset is not None:
            return 
        meta = self._meta
        if self.fargs:
            self.simple = not self.filter_sets
            fargs = self.aggregate(self.fargs)
        else:
            fargs = None
        if self.eargs:
            self.simple = False
            eargs = self.aggregate(self.eargs)
        else:
            eargs = None
        if self.queries:
            self.simple = False
        self.qset = self._meta.cursor.Query(self, fargs, eargs,
                                            queries = self.queries)
    
    def aggregate(self, kwargs):
        return sorted(self._aggregate(kwargs), key = lambda x : x.name)
        
    def _aggregate(self, kwargs):
        '''Aggregate lookup parameters.'''
        meta    = self._meta
        fields  = meta.dfields
        for name,value in kwargs.items():
            names = name.split('__')
            N = len(names)
            lookup = 'in'
            # simple lookup for example filter(name = 'pippo')
            if N == 1:
                if name not in fields:
                    raise QuerySetError('Could not filter on model "{0}".\
 Field "{1}" does not exist.'.format(meta,name))
                field = fields[name]
                value = (field.serialize(value),)
                unique = field.unique
            # group lookup filter(name_in ['pippo','luca'])
            elif N == 2 and names[1] in self.lookups:
                name = names[0]
                if name not in fields:
                    raise QuerySetError("Could not filter.\
 Field %s not defined.".format(name))
                field = fields[name]
                value = tuple((field.serialize(v) for v in value))
                unique = field.unique
                lookup = names[1]
            else: 
                # Nested lookup. Not available yet!
                raise NotImplementedError("Nested lookup is not yet available")
            
            if not field.index:
                raise QuerySetError("Field %s is not an index.\
 Cannot query." % name)
            elif value:
                self.simple = self.simple and unique 
                yield queryarg(name,value,unique,lookup)
        
    def items(self, slic = None):
        '''Generator of instances in queryset.'''
        if self._seq is not None:
            for m in self._seq:
                yield m
        else:
            self._buildquery()
            seq = self._seq = []
            for m in self.qset.items(slic):
                seq.append(m)
                yield m
                
    def aslist(self, slic = None):
        '''Return python ``list`` of elements in queryset'''
        if self._seq is None:
            return list(self.items(slic))
        return self._seq
    
    def delete(self, transaction = None):
        '''Delete all the element in the queryset'''
        if self.count():
            commit = False
            if not transaction:
                commit = True
                transaction = self._meta.cursor.transaction()
            for el in self:
                el.delete(transaction)
            if commit:
                transaction.commit()
        self.clear()
            

class Manager(object):
    '''A manager class for stdnet models. Each :class:`stdnet.orm.StdModel`
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
    def get(self, **kwargs):
        qs = self.filter(**kwargs)
        return qs.get()
    
    def get_or_create(self, **kwargs):
        '''Get an object. If it does not exists, it creates one'''
        try:
            res = self.get(**kwargs)
            created = False
        except ObjectNotFound:
            res = self.model(**kwargs)
            res.save()
            created = True
        return res,created
    
    def filter(self, filter_sets = None, **kwargs):
        '''Create a new :class:`QuerySet` with limiting clauses corresponding to
where or limit in a SQL select statement.

:parameter filter_sets: optional list of set ids to filter.
    Used by the :class:`stdnet.orm.query.M2MRelatedManager`.
:parameter kwargs: dictionaris of limiting clauses.
:rtype: a :class:`stdnet.orm.query.QuerySet` instance.'''
        return QuerySet(self._meta, fargs = kwargs,
                        filter_sets = filter_sets)
    
    def from_queries(self, queries):
        return QuerySet(self._meta, queries = queries)
    
    def exclude(self, **kwargs):
        '''Create a new :class:`QuerySet` containing objects that do not
match the given lookup parameters *kwargs*.

:parameter kwargs: dictionaris of lookup parameters to exclude from the query.
:rtype: a :class:`stdnet.orm.query.QuerySet` instance.'''
        return QuerySet(self._meta, eargs = kwargs)
    
    def search(self, text):
        return QuerySet(self._meta).search(text)
    
    def empty(self):
        ''''''
        return QuerySet(self._meta, empty = True)
    
    def all(self):
        '''Return a :class:`QuerySet` which retrieve all instances\
 of the model. This is a proxy of the :meth:`filter` method
 used without parameters.'''
        return self.filter()
    
    def _setmodel(self, model):
        self.model = model
        self._meta = model._meta
    
    
class BaseRelatedManager(Manager):
    
    def __init__(self, model, instance = None):
        self._setmodel(model)
        self.related_instance = instance
        
        
class RelatedManager(BaseRelatedManager):
    '''A specialized :class:`stdnet.orm.query.Manager` for handling
one-to-many relationships under the hood.
If a model has a :class:`stdnet.orm.ForeignKey` field, instances of
that model will have access to the related (foreign) objects
via a simple attribute of the model.'''
    def __init__(self, model, related_fieldname, instance = None):
        super(RelatedManager,self).__init__(model,instance)
        self.related_fieldname = related_fieldname
    
    def __get__(self, instance, instance_type=None):
        return self.__class__(self.model,self.related_fieldname,
                              instance = instance)
    
    def _get_field(self):
        return self.related_instance._meta.dfields[self.fieldname]
    field = property(_get_field)
    
    def get_related_object(self, model, id):
        return model.objects.get(id = id)
        
    def filter(self, **kwargs):
        if self.related_instance:
            kwargs[self.related_fieldname] = self.related_instance
            return super(RelatedManager,self).filter(**kwargs)
        else:
            raise QuerySetError('Related manager can be accessd only from\
 an instance of its related model.')
        

class M2MRelatedManager(BaseRelatedManager):
    '''A specialized :class:`stdnet.orm.query.Manager` for handling
many-tomany relationships under the hood.
When a model has a :class:`stdnet.orm.ManyToManyField`, instances
of that model will have access to the related objects 
via a simple attribute of the model.'''
    def __init__(self, model, st, to_name, instance):
        super(M2MRelatedManager,self).__init__(model,instance)
        self.st = st
        self.to_name = to_name
    
    def add(self, value, transaction = None):
        '''Add *value*, an instance of ``self.model``, to the set.'''
        if not isinstance(value,self.model):
            raise FieldValueError(
                        '%s is not an instance of %s' % (value,self.model))
        trans = transaction or get_transaction(self.related_instance,
                                               value)
        # Get the related manager
        related = getattr(value,self.to_name)
        self.st.add(value, transaction = trans)
        related.st.add(self.related_instance, transaction = trans)
        # If not part of a wider transaction, commit changes
        if not transaction:
            trans.commit()
            
    def remove(self, value, transaction = None):
        '''Remove *value*, an instance of ``self.model`` from the set of
elements contained by the field.'''
        if not isinstance(value,self.model):
            raise FieldValueError(
                        '%s is not an instance of %s' % (value,self.to._meta))
        trans = transaction or get_transaction(self.related_instance,
                                               value)
        related = getattr(value,self.to_name)
        self.st.discard(value, transaction = trans)
        related.st.discard(self.related_instance, transaction = trans)
        # If not part of a wider transaction, commit changes
        if not transaction:
            trans.commit()
        
    def filter(self, **kwargs):
        '''Filter instances of related model.'''
        kwargs['filter_sets'] = [self.st.id]
        return super(M2MRelatedManager,self).filter(**kwargs)
        

class UnregisteredManager(object):
    
    def __init__(self, model):
        self.model = model
        
    def __getattr__(self, name):
        raise ModelNotRegistered('Model %s is not registered with\
 a backend' % self.model.__name__)


